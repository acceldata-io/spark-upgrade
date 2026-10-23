"""`from pyspark.sql.functions import *` binds more names on 3.5.5 than on 2.4.8.

2.4.8's `pyspark.sql.functions` had an `__all__`; 3.5.5's has none, so its
star import binds every public module global: 255 names 2.4.8's did not
(listed below, from a live import of each). Three kinds of 2.4 code change meaning:

  - A Python builtin the star import now shadows: only `filter`. The other
    builtins it binds (`abs`, `max`, `min`, `round`, `sum`, ...) 2.4.8 already
    exported. Each reference is rewritten to `builtins.filter`, with
    `import builtins`.
  - A name the module imported before the star import (`from functools import
    reduce`, `from math import pi`, `from numpy import median`): 3.5.5's star
    import replaces it with Spark's function. The import is repeated right
    after the star import, so the name is bound as in 2.4 -- an import of a
    module already loaded is idempotent.
  - A name the module defined or assigned before the star import: replaced the
    same way, and a definition cannot be repeated. Reported.

Names whose 3.5.5 binding is the object the module already had (`import sys`,
`from typing import cast`, `from pyspark.sql.types import StringType`) are
not touched. A name bound only after the star import means the same on both
versions.
"""
from __future__ import annotations

import libcst as cst
from libcst.helpers import get_full_name_for_node
from libcst.metadata import BuiltinAssignment, ImportAssignment, PositionProvider, QualifiedNameProvider, ScopeProvider

from spark_upgrade_rules import _cst
from spark_upgrade_rules.detector import Fixer

# `from pyspark.sql.functions import *` on 3.5.5 minus the same on 2.4.8, each
# run on that version. (2.4.8's `basestring` is the one name it lost.)
NEW_STAR_NAMES = frozenset("""
Any ArrayType Callable Column DataFrame DataType Dict Iterable JVMView List Optional PySparkTypeError PySparkValueError
SparkContext StringType StructType TYPE_CHECKING Tuple Type Union UserDefinedFunction UserDefinedTableFunction ValuesView
acosh aes_decrypt aes_encrypt aggregate any_value approx_percentile array_agg array_append array_compact array_insert
array_prepend array_size asinh assert_true atanh bit_and bit_count bit_get bit_length bit_or bit_xor bitmap_bit_position
bitmap_bucket_number bitmap_construct_agg bitmap_count bitmap_or_agg bitwise_not bool_and bool_or btrim bucket
call_function call_udf cardinality cast ceiling char char_length character_length contains convert_timezone cot
count_distinct count_if count_min_sketch csc curdate current_catalog current_database current_schema current_timezone
current_user date_diff date_from_unix_date date_part dateadd datepart day days decimal e elt endswith equal_null every
exists extract filter find_in_set first_value forall from_csv functools get get_active_spark_context getbit has_numpy
histogram_numeric hll_sketch_agg hll_sketch_estimate hll_union hll_union_agg hours ifnull ilike inline inline_outer
input_file_block_length input_file_block_start inspect isnotnull java_method json_array_length json_object_keys
last_value lcase left like ln localtimestamp make_date make_dt_interval make_interval make_timestamp make_timestamp_ltz
make_timestamp_ntz make_ym_interval map_contains_key map_entries map_filter map_zip_with mask max_by median min_by mode
months named_struct negate negative now nth_value nullif nvl nvl2 octet_length overlay overload parse_url percentile
percentile_approx pi pmod position positive power printf product raise_error reduce reflect regexp regexp_count
regexp_extract_all regexp_instr regexp_like regexp_substr regr_avgx regr_avgy regr_count regr_intercept regr_r2
regr_slope regr_sxx regr_sxy regr_syy replace right rlike schema_of_csv sec sentences session_window sha shiftleft
shiftright shiftrightunsigned sign some split_part stack startswith std str_to_map substr sum_distinct sys
timestamp_micros timestamp_millis timestamp_seconds to_binary to_char to_csv to_number to_str to_timestamp_ltz
to_timestamp_ntz to_unix_timestamp to_varchar transform transform_keys transform_values try_add try_aes_decrypt try_avg
try_divide try_element_at try_multiply try_remote_functions try_subtract try_sum try_to_binary try_to_number
try_to_timestamp typeof ucase udtf unix_date unix_micros unix_millis unix_seconds unwrap_udt url_decode url_encode user
version warnings weekday width_bucket window_time xpath xpath_boolean xpath_double xpath_float xpath_int xpath_long
xpath_number xpath_short xpath_string xxhash64 years zip_with
""".split())

# The new names 3.5.5's functions module imports rather than defines, and from
# where: a module that imported the same thing gets the same object back.
_REEXPORTED = {
    **{n: n for n in ("decimal", "functools", "inspect", "sys", "warnings")},
    **{n: f"typing.{n}" for n in ("Any", "Callable", "Dict", "Iterable", "List", "Optional", "TYPE_CHECKING", "Tuple",
                                  "Type", "Union", "ValuesView", "cast", "overload")},
    "JVMView": "py4j.java_gateway.JVMView",
    **{n: "pyspark." + n for n in ("ArrayType", "Column", "DataFrame", "DataType", "PySparkTypeError", "PySparkValueError",
                                   "SparkContext", "StringType", "StructType", "UserDefinedFunction",
                                   "UserDefinedTableFunction", "get_active_spark_context", "has_numpy", "to_str",
                                   "try_remote_functions")},
}

# The builtins in NEW_STAR_NAMES.
SHADOWED_BUILTINS = ("filter",)


def _is_star(stmt: cst.CSTNode) -> bool:
    return isinstance(stmt, cst.SimpleStatementLine) and any(
        isinstance(s, cst.ImportFrom) and isinstance(s.names, cst.ImportStar) and s.module is not None
        and get_full_name_for_node(s.module) == _cst.FUNCTIONS for s in stmt.body)


def _imports(stmt: cst.CSTNode) -> list[cst.Import | cst.ImportFrom]:
    if not isinstance(stmt, cst.SimpleStatementLine):
        return []
    return [s for s in stmt.body if isinstance(s, (cst.Import, cst.ImportFrom))]


def _bound(alias: cst.ImportAlias) -> str:
    """The name an import alias binds: `as x`, else the first dotted part."""
    if alias.asname is not None:
        return alias.asname.name.value
    return get_full_name_for_node(alias.name).split(".")[0]


class StarImportShadowsFilter(Fixer):
    """`from pyspark.sql.functions import *`: on 3.5.5 it also binds `filter`
    and 254 other names 2.4.8's did not export. See the module docstring."""

    rule_id = "StarImportShadowsFilter"
    METADATA_DEPENDENCIES = (PositionProvider, QualifiedNameProvider, ScopeProvider)

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        body = ctx.module.body
        stars = [i for i, s in enumerate(body) if _is_star(s)]
        self.star_index = stars[-1] if stars else None  # a top-level star import; the last, if several
        self.builtin_refs: set[int] = set()
        self.reimports: list[tuple[int, cst.SimpleStatementLine]] = []  # (line of the original import, the repeat)
        self.add_builtins_import = False

    def visit_Module(self, node: cst.Module) -> bool:
        if not self.ctx.star_functions:
            return False
        scope = self.get_metadata(ScopeProvider, node)
        star_line = self._star_line(node)
        for name in sorted(NEW_STAR_NAMES):
            assignments = [a for a in scope[name] if not isinstance(a, BuiltinAssignment)]
            if not assignments and name in SHADOWED_BUILTINS:
                self._builtin(scope, name)
            elif assignments and star_line is not None and any(a.references for a in assignments):
                self._rebound_before_star(node, name, assignments, star_line)
        return True

    def _star_line(self, module: cst.Module) -> int | None:
        if self.star_index is not None:
            return self.get_metadata(PositionProvider, module.body[self.star_index]).start.line
        return None

    def _builtin(self, scope, name: str) -> None:
        refs = [r.node for a in scope[name] if isinstance(a, BuiltinAssignment) for r in a.references
                if isinstance(r.node, cst.Name)]
        if not refs:
            return
        bound = [a for a in scope["builtins"] if not isinstance(a, BuiltinAssignment)]
        plain = all(isinstance(a, ImportAssignment) and isinstance(a.node, cst.Import) and
                    any(get_full_name_for_node(al.name) == "builtins" and al.asname is None for al in a.node.names)
                    for a in bound)
        if not plain:
            for ref in refs:
                self.report(ref, self._builtin_message(name) + " The file binds `builtins` to something else, so it is "
                                 f"not rewritten: call builtins.{name} through `import builtins`.", "high", tier=3)
            return
        self.builtin_refs.update(id(r) for r in refs)
        self.add_builtins_import = not bound

    @staticmethod
    def _builtin_message(name: str) -> str:
        return (f"`{name}` here is Python's builtin under `from pyspark.sql.functions import *`. 2.4.8's star import did "
                f"not export {name}; 3.5.5's does (pyspark.sql.functions.{name}), so this would call Spark's function -- "
                "a type error or a Column, not an iterator.")

    def _rebound_before_star(self, module: cst.Module, name: str, assignments, star_line: int) -> None:
        def line(a) -> int:
            return self.get_metadata(PositionProvider, a.node).start.line
        before = sorted((a for a in assignments if line(a) < star_line), key=line)
        if not before:
            return
        last = before[-1]
        star = module.body[self.star_index]
        if isinstance(last, ImportAssignment) and any(last.node is s for stmt in module.body[:self.star_index]
                                                      for s in _imports(stmt)):
            imp = last.node
            aliases = [a for a in imp.names if _bound(a) == name]
            if not aliases or self._same_object(imp, aliases[-1], name):
                return
            alias = aliases[-1].with_changes(comma=cst.MaybeSentinel.DEFAULT)
            again = cst.SimpleStatementLine(body=[imp.with_changes(names=[alias], **(
                {"lpar": None, "rpar": None} if isinstance(imp, cst.ImportFrom) else {}))])
            self.reimports.append((line(last), again))
            self.report(star, f"`{name}`, imported on line {line(last)}, is replaced by this star import on 3.5.5, which "
                              f"exports pyspark.sql.functions.{name}; 2.4.8's did not. The import is repeated after the "
                              f"star import, so `{name}` is what it was on 2.4.", "high", tier=1,
                        fix=f"{self.code(star)} -> {self.code(star)}; {self.code(again)}")
        else:
            self.report(last.node if isinstance(last.node, cst.CSTNode) else star,
                        f"`{name}`, defined on line {line(last)}, is replaced by the star import on line {star_line} on "
                        f"3.5.5, which exports pyspark.sql.functions.{name}; 2.4.8's did not. Move the definition below "
                        "the star import, or import the Spark functions by name.", "high", tier=3)

    @staticmethod
    def _same_object(imp: cst.Import | cst.ImportFrom, alias: cst.ImportAlias, name: str) -> bool:
        origin = _REEXPORTED.get(name)
        if origin is None or alias.asname is not None:
            return False
        if isinstance(imp, cst.Import):
            return get_full_name_for_node(alias.name) == origin
        if imp.relative or imp.module is None:
            return False
        qualified = f"{get_full_name_for_node(imp.module)}.{get_full_name_for_node(alias.name)}"
        return qualified == origin or (origin.startswith("pyspark.") and qualified.startswith("pyspark."))

    def leave_Name(self, original_node: cst.Name, updated_node: cst.Name) -> cst.BaseExpression:
        if id(original_node) not in self.builtin_refs:
            return updated_node
        new = cst.Attribute(value=cst.Name("builtins"), attr=cst.Name(original_node.value))
        return self.rewrite(original_node, new, self._builtin_message(original_node.value) + " Rewritten to "
                            f"builtins.{original_node.value}, the builtin 2.4 called.")

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        if not (self.reimports or (self.builtin_refs and self.add_builtins_import)):
            return updated_node
        body = list(updated_node.body)
        if self.reimports:
            body[self.star_index + 1:self.star_index + 1] = [stmt for _, stmt in sorted(self.reimports, key=lambda r: r[0])]
        if self.builtin_refs and self.add_builtins_import:
            body.insert(_cst.after_leading_imports(body), cst.parse_statement("import builtins\n"))
        return updated_node.with_changes(body=body)
