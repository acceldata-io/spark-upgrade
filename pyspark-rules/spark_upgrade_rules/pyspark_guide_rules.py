"""The PySpark upgrade guide's own 3.3 -> 3.4 core changes (spark-migrate-cli
PYSPARK.md G14, SS10 R2 and R4) -- PySpark-only, so there is no Scala rule to
port. Each claim was run against a real PySpark 3.5.5 session, with the
legacy flag on to reproduce the pre-3.4 behaviour (PYSPARK.md SS8.13).
"""
from __future__ import annotations

import libcst as cst
import libcst.matchers as m

from spark_upgrade_rules import _cst
from spark_upgrade_rules.detector import Detector

_FIX = ("Pass an explicit schema, or set spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled=true "
        "-- which restores the old rule for every array in the job.")


def _literal_kind(node: cst.BaseExpression) -> str | None:
    """The type a literal infers as, for comparing array elements; None for a
    None literal or a non-literal."""
    value = _cst.literal_value(node)
    if value is None or value == ("const", "None"):
        return None
    if value[0] == "const":
        return "bool"
    return value[0]


class ArrayTypeSchemaInference(Detector):
    """Schema inference from Python data -- `createDataFrame(data)` with no
    schema (or only column names), `rdd.toDF()`. From 3.4 an array's element
    type is merged across all elements, where 3.3 took the first element's.
    Probed on 3.5.5: `[1, 1.5]` inferred array<bigint> before and now fails
    (CANNOT_INFER_TYPE_FOR_FIELD); `[{"a": 1}, {"b": "x"}]` was
    map<string,bigint> and is now map<string,string>. `spark.read.json` is not
    affected -- JSON inference is the JVM's. A uniform array infers the same
    either way.

    When the rows are literals in the code, the change is decidable: a
    collection mixing element types is flagged high, and literal rows with none
    are not flagged. Otherwise the data is not visible, and the call site is
    flagged low -- one repo-wide decision, since one config settles them all."""

    rule_id = "ArrayTypeSchemaInference"

    def visit_Call(self, node: cst.Call) -> None:
        name = _cst.method_name(node)
        if name == "createDataFrame":
            data, schema = _cst.argument(node, 0, "data"), _cst.argument(node, 1, "schema")
        elif name == "toDF":
            data, schema = None, _cst.argument(node, 0, "schema")
            if any(a.star == "*" for a in node.args) or (schema is not None and self.string(schema) is not None):
                schema = None  # toDF("a", "b") or toDF(*cols): names, still inferred
        else:
            return
        if data is None and name == "createDataFrame":
            return
        if schema is not None and not self._names_only(schema):
            return
        rows = self._literal_rows(data)
        if rows is None:
            self.report(node, f"{name}(...) infers its schema from Python data this rule cannot see. From Spark 3.4 an "
                              "array's or map's element type is merged across all elements, not taken from the first: "
                              "a mix such as [1, 1.5] now fails to infer, and [{'a': 1}, {'b': 'x'}] becomes "
                              f"map<string,string> instead of map<string,bigint>. {_FIX}", "low")
            return
        mixed = self._mixed_collection(rows)
        if mixed is not None:
            self.report(node, f"{name}(...) infers its schema from rows containing {mixed}. Spark 3.4 merges element "
                              "types across the whole collection, where 2.4-3.3 used the first element's: this one "
                              f"now infers differently or fails. {_FIX}", "high")

    def _names_only(self, schema: cst.BaseExpression) -> bool:
        """A list of column names: the types are still inferred."""
        return isinstance(schema, (cst.List, cst.Tuple)) and all(
            _cst.string_value(el.value) is not None for el in schema.elements)

    def _literal_rows(self, data: cst.BaseExpression | None) -> list | None:
        if isinstance(data, cst.Name):
            data = self.ctx.bindings.expression(data.value)
        if isinstance(data, (cst.List, cst.Tuple)) and not any(isinstance(e, cst.StarredElement) for e in data.elements):
            return [e.value for e in data.elements]
        return None

    def _mixed_collection(self, rows: list) -> str | None:
        stack = list(rows)
        while stack:
            n = stack.pop()
            if isinstance(n, cst.Call) and isinstance(n.func, cst.Name) and n.func.value == "Row":
                stack.extend(a.value for a in n.args)
                continue
            if isinstance(n, cst.Tuple):
                stack.extend(e.value for e in n.elements)
                continue
            if isinstance(n, (cst.List, cst.Set)):
                kinds = {k for k in (_literal_kind(e.value) for e in n.elements) if k is not None}
                if len(kinds) > 1:  # e.g. int and float: 3.4's merge fails where 2.4 took the first
                    return f"an array mixing {', '.join(sorted(kinds))} elements"
                dict_kinds = {k for e in n.elements if isinstance(e.value, cst.Dict)
                              for el in e.value.elements if isinstance(el, cst.DictElement)
                              for k in [_literal_kind(el.value)] if k is not None}
                if len(dict_kinds) > 1:
                    return f"an array of maps whose values mix {', '.join(sorted(dict_kinds))}"
                stack.extend(e.value for e in n.elements)
                continue
            if isinstance(n, cst.Dict):
                stack.extend(el.value for el in n.elements if isinstance(el, cst.DictElement))
        return None


_SHIPS_WORK = {"map", "flatMap", "mapPartitions", "mapPartitionsWithIndex", "foreach", "foreachPartition",
               "mapValues", "flatMapValues", "filter", "reduceByKey", "aggregateByKey", "combineByKey"}


class NamedtupleCloudpickle(Detector):
    """A module-level `collections.namedtuple` in a file that ships closures to
    executors (RDD functions -- a lambda or a function defined in the file --
    and UDFs). Spark 3.4 removed PySpark's namedtuple
    patch, which rebuilt a namedtuple on the executor from its name and
    fields; cloudpickle pickles a module-level class *by reference*, so the
    executor has to import its module. Probed on 3.5.5: retail's
    `partner_feeds.visits` fails with ModuleNotFoundError when the package is
    not shipped to executors, and runs with PYSPARK_ENABLE_NAMEDTUPLE_PATCH=1.
    A namedtuple defined in a function, or in the entry script (`__main__`), is
    pickled by value and is unaffected -- the first is not flagged; the second
    cannot be told apart from an imported module, hence medium. One finding per
    file."""

    rule_id = "NamedtupleCloudpickle"

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        self.first: cst.Call | None = None
        self.ships = False
        self.depth = 0
        # Functions defined anywhere in the file: `rdd.mapPartitions(_hits)` ships
        # a named function as surely as a lambda.
        self.defined = {n.name.value for n in m.findall(ctx.module, m.FunctionDef())}

    def _shipped(self, arg: cst.Arg) -> bool:
        return isinstance(arg.value, cst.Lambda) or (isinstance(arg.value, cst.Name) and arg.value.value in self.defined)

    def visit_FunctionDef(self, node: cst.FunctionDef) -> None:
        self.depth += 1

    def leave_FunctionDef(self, original_node: cst.FunctionDef) -> None:
        self.depth -= 1

    def visit_ClassDef(self, node: cst.ClassDef) -> None:
        self.depth += 1

    def leave_ClassDef(self, original_node: cst.ClassDef) -> None:
        self.depth -= 1

    def visit_Call(self, node: cst.Call) -> None:
        q = self.qualified(node.func)
        if self.first is None and self.depth == 0 and ("collections.namedtuple" in q or "typing.NamedTuple" in q):
            self.first = node
        name = _cst.method_name(node)
        if (name in _SHIPS_WORK and any(self._shipped(a) for a in node.args)) or \
                name in ("foreach", "foreachPartition") or self.function(node, "udf", "pandas_udf"):
            self.ships = True

    def visit_Decorator(self, node: cst.Decorator) -> None:
        target = node.decorator.func if isinstance(node.decorator, cst.Call) else node.decorator
        if any(q.endswith((".udf", ".pandas_udf")) for q in self.qualified(target)):
            self.ships = True

    def leave_Module(self, original_node: cst.Module) -> None:
        if self.first is not None and self.ships:
            self.report(self.first, "A module-level namedtuple in a file that ships closures to executors. Spark 3.4 removed "
                                    "PySpark's namedtuple patch, so cloudpickle pickles it by reference and every executor "
                                    "must import this module: ModuleNotFoundError unless the package is shipped (--py-files, "
                                    "a zip, or installed on the nodes). Ship it, or set PYSPARK_ENABLE_NAMEDTUPLE_PATCH=1 on "
                                    "the driver and executors -- an environment variable, so codegen cannot inject it. "
                                    "Unaffected if this file is the entry script.", "medium")
