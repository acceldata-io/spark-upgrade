"""Koalas -> pandas API on Spark, and the two pandas-on-Spark changes the port
makes reachable (spark-migrate-cli PYSPARK.md SS2.7).

A 2.4 repo's pandas-on-Spark is Koalas, which supports Spark 3.1 and below;
Spark 3.2 took the same code in-tree as `pyspark.pandas`. The port is the
documented migration, and the parts of it that are exact are rewritten:

  KoalasToPandasOnSpark       the import, `databricks.koalas.X`, Spark's
                              `.to_koalas()` and the `.koalas` accessor; and the
                              17 members Koalas 1.8.2 kept as deprecated aliases
                              and pyspark.pandas 3.5.5 does not have, each to the
                              call the alias made (read in Koalas' own source).
  PandasOnSparkDropAxis       `drop(label)` dropped a column in Koalas and 3.2;
                              from 3.3 it looks for an index label and, probed on
                              3.5.5, silently drops nothing. `axis=1` restores it.
  PandasOnSparkSqlFormatter   `ks.sql("... {name} ...")` filled placeholders from
                              the caller's variables; from 3.3 it is the standard
                              formatter and raises KeyError. Passing each name as
                              a keyword argument gives the same result in both
                              modes (probed).

Diffed, not transcribed: Koalas 1.8.2's namespace and options against a live
3.5.5 `pyspark.pandas` import -- every top-level name and option is there; the
DataFrame/Series/Index members that are not are the aliases below. What the
port does not make equal -- the 3.3 -> 3.4 changes -- the PandasOnSpark*
detectors report.
"""
from __future__ import annotations

import string

import libcst as cst
from libcst.helpers import get_full_name_for_node

from spark_upgrade_rules import _cst
from spark_upgrade_rules.detector import Fixer
from spark_upgrade_rules.pandas_on_spark_rules import Provenance

KOALAS = "databricks.koalas"
PANDAS_ON_SPARK = "pyspark.pandas"
# Koalas submodules pyspark.pandas 3.5.5 has under the same name.
SUBMODULES = {"accessors", "base", "categorical", "config", "datetimes", "exceptions", "extensions", "frame", "generic",
              "groupby", "indexes", "indexing", "internal", "missing", "mlflow", "namespace", "numpy_compat", "plot",
              "series", "spark", "strings", "typedef", "usage_logging", "utils", "window"}

# Koalas 1.8.2's deprecated aliases with no pyspark.pandas 3.5.5 counterpart:
# name -> (accessor or None, the method it called). An empty accessor is the
# frame itself. `unique` names exist on no Spark DataFrame, so they are
# rewritten wherever the frame is not visibly plain pandas or Spark; the
# others only on a frame provably from Koalas.
ALIASES = {
    "apply_batch": ("pandas_on_spark", "apply_batch", True),
    "map_in_pandas": ("pandas_on_spark", "apply_batch", True),
    "transform_batch": ("pandas_on_spark", "transform_batch", True),
    "spark_schema": ("spark", "schema", True),
    "print_schema": ("spark", "print_schema", True),
    "cache": ("spark", "cache", False),
    "persist": ("spark", "persist", False),
    "hint": ("spark", "hint", False),
    "explain": ("spark", "explain", False),
    "toPandas": ("", "to_pandas", False),
    "alias": ("", "rename", False),
}


def _dotted(name: str) -> cst.BaseExpression:
    return cst.parse_expression(name)


class KoalasToPandasOnSpark(Provenance, Fixer):
    rule_id = "KoalasToPandasOnSpark"

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        self.uses_koalas = any(n == KOALAS or n.startswith(KOALAS + ".") for n in _imported_modules(ctx.module))

    def _target(self, module: str) -> str | None:
        """The pyspark.pandas module for a Koalas one, or None if there is none."""
        if module == KOALAS:
            return PANDAS_ON_SPARK
        sub = module[len(KOALAS) + 1:]
        return f"{PANDAS_ON_SPARK}.{sub}" if sub.split(".")[0] in SUBMODULES else None

    # -- imports ------------------------------------------------------------
    def leave_Import(self, original_node: cst.Import, updated_node: cst.Import) -> cst.Import:
        names, changed = [], False
        for before, alias in zip(original_node.names, updated_node.names):
            module = get_full_name_for_node(before.name)
            if module and (module == KOALAS or module.startswith(KOALAS + ".")):
                target = self._target(module)
                if target is None:
                    self._missing(original_node, module)
                else:
                    alias = alias.with_changes(name=_dotted(target))
                    changed = True
            names.append(alias)
        if not changed:
            return updated_node
        return self.rewrite(original_node, updated_node.with_changes(names=names), self._import_message())

    def leave_ImportFrom(self, original_node: cst.ImportFrom, updated_node: cst.ImportFrom) -> cst.ImportFrom:
        module = get_full_name_for_node(original_node.module) if original_node.module else None
        if module == "databricks" and not isinstance(original_node.names, cst.ImportStar) and \
                any(get_full_name_for_node(a.name) == "koalas" for a in original_node.names):
            # `from databricks import koalas as ks`
            names = [a.with_changes(name=cst.Name("pandas")) if get_full_name_for_node(a.name) == "koalas" else a
                     for a in updated_node.names]
            if any(get_full_name_for_node(a.name) == "koalas" and a.asname is None for a in original_node.names):
                self.report(original_node, "from databricks import koalas: the name `koalas` is used below; import it as "
                                           "`from pyspark import pandas as koalas` or rename the uses by hand.", "high", tier=3)
                return updated_node
            return self.rewrite(original_node, updated_node.with_changes(module=cst.Name("pyspark"), names=names),
                                self._import_message())
        if not module or not (module == KOALAS or module.startswith(KOALAS + ".")):
            return updated_node
        target = self._target(module)
        if target is None:
            self._missing(original_node, module)
            return updated_node
        return self.rewrite(original_node, updated_node.with_changes(module=_dotted(target)), self._import_message())

    def _import_message(self) -> str:
        return ("Koalas supports Spark 3.1 and below; Spark 3.2 took it in-tree as pyspark.pandas, the same API under a new "
                "name. Rewritten to import pyspark.pandas (the alias the code uses is kept), with databricks.koalas.X, "
                "to_koalas() and the .koalas accessor renamed to match. Behaviour changes after 3.2 are reported by the "
                "PandasOnSpark* rules.")

    def _missing(self, node: cst.CSTNode, module: str) -> None:
        self.report(node, f"{module} has no pyspark.pandas counterpart (Koalas' ml, sql, testing and version modules were "
                          "not carried over). Move this code off it by hand.", "high", tier=3)

    # -- references -----------------------------------------------------------
    def leave_Attribute(self, original_node: cst.Attribute, updated_node: cst.Attribute) -> cst.BaseExpression:
        full = get_full_name_for_node(original_node)
        # `databricks.koalas.DataFrame` after `import databricks.koalas`.
        if full and full.startswith(KOALAS + ".") and isinstance(original_node.value, cst.Attribute) and \
                get_full_name_for_node(original_node.value) == KOALAS and any(
                    q.startswith(KOALAS) for q in self.qualified(original_node)):
            return updated_node.with_changes(value=_dotted(PANDAS_ON_SPARK))
        if not self.uses_koalas:
            return updated_node
        if original_node.attr.value == "koalas" and get_full_name_for_node(original_node.value) != "databricks":
            return self.rewrite(original_node, updated_node.with_changes(attr=cst.Name("pandas_on_spark")),
                                "The .koalas accessor is .pandas_on_spark in pyspark.pandas (the old name is a deprecated "
                                "alias on 3.5.5). Rewritten.")
        if original_node.attr.value == "spark_type" and self.provenance(original_node.value) not in ("pd", "spark"):
            return self.rewrite(original_node, cst.Attribute(value=cst.Attribute(value=updated_node.value, attr=cst.Name("spark")),
                                                             attr=cst.Name("data_type")),
                                "Series/Index.spark_type was a deprecated Koalas alias of .spark.data_type, which pyspark.pandas "
                                "does not keep -- AttributeError. Rewritten to what it returned.")
        return updated_node

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.BaseExpression:
        name = _cst.method_name(original_node)
        if name is None or not self.uses_koalas:
            return updated_node
        recv = _cst.receiver(original_node)
        kind = self.provenance(recv)
        if name == "to_koalas":
            if kind == "ps":
                return self.rewrite(original_node, updated_node.func.value,
                                    "to_koalas() on a Koalas frame returned the frame itself; pyspark.pandas has no "
                                    "to_koalas. Rewritten to the frame.")
            return self.rewrite(original_node, updated_node.with_changes(
                func=updated_node.func.with_changes(attr=cst.Name("pandas_api"))),
                "Spark's DataFrame.to_koalas() came from importing Koalas; on 3.5.5 it is a deprecated alias of "
                "pandas_api(). Rewritten to pandas_api().")
        if name not in ALIASES or kind in ("pd", "spark"):
            return updated_node
        # `kdf.koalas.apply_batch(f)` is the accessor's own method, not the alias.
        if isinstance(recv, cst.Attribute) and recv.attr.value in ("koalas", "pandas_on_spark", "spark"):
            return updated_node
        accessor, method, unique = ALIASES[name]
        if not unique and kind != "ps":
            return updated_node
        base = updated_node.func.value
        if accessor:
            base = cst.Attribute(value=base, attr=cst.Name(accessor))
        new = updated_node.with_changes(func=cst.Attribute(value=base, attr=cst.Name(method)))
        target = f".{accessor + '.' if accessor else ''}{method}(...)"
        return self.rewrite(original_node, new,
                            f".{name}(...) was a deprecated Koalas alias of {target}, which pyspark.pandas does not keep -- "
                            f"AttributeError on 3.5.5. Rewritten to the call the alias made.")


def _imported_modules(module: cst.Module) -> list[str]:
    out = []
    for node in _walk(module):
        if isinstance(node, cst.Import):
            out.extend(get_full_name_for_node(a.name) or "" for a in node.names)
        elif isinstance(node, cst.ImportFrom) and node.module is not None:
            base = get_full_name_for_node(node.module) or ""
            out.append(base)
            if base == "databricks" and not isinstance(node.names, cst.ImportStar):
                out.extend(f"databricks.{get_full_name_for_node(a.name)}" for a in node.names)
    return out


def _walk(node: cst.CSTNode):
    stack = [node]
    while stack:
        n = stack.pop()
        yield n
        stack.extend(n.children)


class PandasOnSparkDropAxis(Provenance, Fixer):
    """Supersedes PySparkler's PY32-33-001, which matched every `.drop(x)` --
    100% false positives on plain Spark (PYSPARK.md SS2.3)."""

    rule_id = "PandasOnSparkDropAxis"

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.BaseExpression:
        if _cst.method_name(original_node) != "drop" or _cst.has_star_args(original_node):
            return updated_node
        args = original_node.args
        keywords = {a.keyword.value for a in args if a.keyword is not None}
        positional = _cst.positional(original_node)
        if keywords & {"axis", "columns", "index", "level"} or len(positional) > 1 or \
                not (len(positional) == 1 or keywords == {"labels"}):
            return updated_node
        recv = _cst.receiver(original_node)
        # `frame["c"].drop(x)` is a Series, whose drop always took index labels.
        if any(isinstance(n, cst.Subscript) for n in _cst.spine(recv)):
            return updated_node
        kind = self.provenance(recv)
        if kind in ("pd", "spark"):
            return updated_node
        message = ("drop(label) on a pandas-on-Spark DataFrame: Koalas and Spark 3.2 dropped the column; from Spark 3.3 the "
                   "default is axis=0, an index label -- and when no row has that label nothing is dropped, silently "
                   "(probed on 3.5.5).")
        if kind != "ps":
            self.report(original_node, message + " The frame's origin is not visible here: if it is pandas-on-Spark, add "
                                                 "axis=1.", "low", tier=3)
            return updated_node
        last = updated_node.args[-1].with_changes(comma=cst.Comma(whitespace_after=cst.SimpleWhitespace(" ")))
        axis = cst.Arg(keyword=cst.Name("axis"), value=cst.Integer("1"),
                       equal=cst.AssignEqual(whitespace_before=cst.SimpleWhitespace(""), whitespace_after=cst.SimpleWhitespace("")))
        return self.rewrite(original_node, updated_node.with_changes(args=[*updated_node.args[:-1], last, axis]),
                            message + " Rewritten with axis=1, the column drop it made before.")


SQL_FUNCTIONS = ("pyspark.pandas.sql", "pyspark.pandas.sql_formatter.sql", "databricks.koalas.sql",
                 "databricks.koalas.sql.sql", "databricks.koalas.namespace.sql")


class PandasOnSparkSqlFormatter(Fixer):
    """The PySpark guide's 3.2 -> 3.3 #1: `pyspark.pandas.sql` follows the
    standard Python string formatter. Koalas (and 3.2) filled `{name}` from
    the caller's globals and locals; probed on 3.5.5, the same call raises
    KeyError unless PYSPARK_PANDAS_SQL_LEGACY=1 -- an environment variable,
    so codegen cannot set it. Passing each placeholder's variable as a keyword
    argument gives the same rows in both modes."""

    rule_id = "PandasOnSparkSqlFormatter"

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.BaseExpression:
        if not any(q in SQL_FUNCTIONS for q in self.qualified(original_node.func)):
            return updated_node
        query = _cst.string_value(_cst.argument(original_node, 0, "query"))
        given = {a.keyword.value for a in original_node.args if a.keyword is not None}
        if query is None:
            return updated_node
        try:
            fields = [(name, spec, conv) for _, name, spec, conv in string.Formatter().parse(query) if name is not None]
        except ValueError:
            return updated_node
        if not fields:
            return updated_node
        message = ("pyspark.pandas.sql follows the standard string formatter from Spark 3.3: a {name} placeholder is no "
                   "longer looked up in the caller's variables, as Koalas did -- KeyError on 3.5.5 (probed).")
        simple = all(name.isidentifier() and not spec and not conv for name, spec, conv in fields)
        if not simple or given & {"globals", "locals"}:
            self.report(original_node, message + " The placeholders are not plain names: pass each value as a keyword "
                                                 "argument by hand, or set PYSPARK_PANDAS_SQL_LEGACY=1.", "high", tier=3)
            return updated_node
        missing = list(dict.fromkeys(name for name, _, _ in fields if name not in given))
        if not missing:
            return updated_node
        args = list(updated_node.args)
        args[-1] = args[-1].with_changes(comma=cst.Comma(whitespace_after=cst.SimpleWhitespace(" ")))
        eq = cst.AssignEqual(whitespace_before=cst.SimpleWhitespace(""), whitespace_after=cst.SimpleWhitespace(""))
        added = [cst.Arg(keyword=cst.Name(n), value=cst.Name(n), equal=eq,
                         comma=cst.Comma(whitespace_after=cst.SimpleWhitespace(" ")) if i < len(missing) - 1 else cst.MaybeSentinel.DEFAULT)
                 for i, n in enumerate(missing)]
        return self.rewrite(original_node, updated_node.with_changes(args=[*args, *added]),
                            message + f" Rewritten to pass {', '.join(missing)} explicitly, which gives the same result.")
