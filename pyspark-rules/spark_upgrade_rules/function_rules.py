"""Families E and F, and grouping_id: built-in functions and types whose
behaviour changed for a literal-decidable argument shape. Ports of the Scala
rules of the same ids; each narrows to the shape that is certain from the
call alone, as the Scala rule does.
"""
from __future__ import annotations

import libcst as cst
from libcst.helpers import get_full_name_for_node

from spark_upgrade_rules import _cst
from spark_upgrade_rules.detector import Detector, Fixer, NameDiscarded


def _collection_args(call: cst.Call) -> list[cst.BaseExpression] | None:
    """`f(a, b)` or `f([a, b])` -> [a, b]: PySpark's collection functions take
    either. None when the arguments are not knowable (`*cols`)."""
    if _cst.has_star_args(call):
        return None
    args = _cst.positional(call)
    if len(args) == 1 and isinstance(args[0], (cst.List, cst.Tuple)):
        if any(el.value is None or isinstance(el, cst.StarredElement) for el in args[0].elements):
            return None
        return [el.value for el in args[0].elements]
    return args


class NegativeDecimalScaleDetect(Detector):
    rule_id = "NegativeDecimalScaleDetect"

    def visit_Call(self, node: cst.Call) -> None:
        if self.type_name(node, "DecimalType") is None:
            return
        scale = _cst.int_value(_cst.argument(node, 1, "scale"))
        if scale is not None and scale < 0:
            self.report(node, f"DecimalType(..., {scale}) has a negative scale. Spark 3.0 rejects it at analysis time by default; "
                              "2.4 allowed it. Use a non-negative scale, or set spark.sql.legacy.allowNegativeScaleOfDecimal=true.")


class DuplicateMapKeyLiteralDetect(Detector):
    rule_id = "DuplicateMapKeyLiteralDetect"

    def _key(self, node: cst.BaseExpression) -> tuple | None:
        """What a map key evaluates to, when that is fixed by the code: `lit(x)`
        is the value x; a bare string is a column name, and the same column
        named twice is the same key twice on every row."""
        if isinstance(node, cst.Call) and self.function(node, "lit"):
            value = _cst.literal_value(_cst.argument(node, 0, "col"))
            return ("lit",) + value if value is not None else None
        name = _cst.string_value(node)
        return ("column", name) if name is not None else None

    def visit_Call(self, node: cst.Call) -> None:
        if self.function(node, "create_map") is None:
            return
        args = _collection_args(node)
        if not args:
            return
        keys = [k for k in (self._key(a) for a in args[0::2]) if k is not None]
        dupes = sorted({k for k in keys if keys.count(k) > 1})
        if dupes:
            shown = ", ".join(f"column {k[1]!r}" if k[0] == "column" else repr(k[2]) for k in dupes)
            self.report(node, f"create_map(...) repeats the key {shown}. Spark 3.0 throws on a duplicate map key "
                              "(spark.sql.mapKeyDedupPolicy=EXCEPTION). In 2.4 the result was undefined -- a lookup returned the "
                              "first value, collect() kept the last -- so no rewrite reproduces it. Remove the duplicate you do "
                              "not mean, or set spark.sql.mapKeyDedupPolicy=LAST_WIN.")


class MapTypeKeyInCreateMapDetect(Detector):
    rule_id = "MapTypeKeyInCreateMapDetect"

    def _is_map(self, node: cst.BaseExpression) -> bool:
        return isinstance(node, cst.Call) and self.function(node, "create_map", "map_from_arrays") is not None

    def visit_Call(self, node: cst.Call) -> None:
        fn = self.function(node, "create_map", "map_from_arrays")
        if fn == "create_map":
            args = _collection_args(node) or []
            hit = any(self._is_map(k) for k in args[0::2])
        elif fn == "map_from_arrays":
            keys = _cst.argument(node, 0, "col1")
            hit = isinstance(keys, cst.Call) and self.function(keys, "array") is not None and \
                any(self._is_map(k) for k in (_collection_args(keys) or []))
        else:
            return
        if hit:
            self.report(node, f"{fn}(...) uses a map as a map key. Spark 3.0 rejects a MapType key at analysis time; 2.4 "
                              "accepted it. No config restores this -- the key's type has to change (map_entries() turns a map "
                              "into an array of structs).")


# Calls that widen an empty collection to their other arguments' type -- probed
# on 3.5.5: coalesce(tags, array()) is array<string> and writes to Parquet, as
# do array_union/concat with a typed array and when/otherwise with a typed
# branch. Only an empty collection whose own type survives -- a bare column, a
# when() with no otherwise -- is array<void> on 3.x.
_COERCING = {"coalesce", "nvl", "ifnull", "array_union", "array_except", "array_intersect", "concat", "map_concat",
             "greatest", "least"}


class EmptyCollectionTypeDetect(Fixer):
    """`array()` / `create_map()` with no elements: NullType elements from 3.0,
    string in 2.4. Rewritten to `.cast(ArrayType(StringType(), False))` /
    `.cast(MapType(StringType(), StringType(), False))` -- exactly 2.4's type,
    nullability included, at the call alone, where the legacy config would
    change every empty collection in the job. A DDL string cast
    (`"array<string>"`) is not exact: it makes the elements nullable, where
    2.4's were not. Probed, the full schema (`dataType.json()`) on 2.4.8 and
    3.5.5, at the top of a select and inside struct and when: identical, and
    the cast array writes to Parquet where the bare one does not.
    Unlike AddMonthsSnapDetect's rewrite, this one is made in any context: Spark
    leaves a cast out of a generated column name, so the name stays `array()`
    -- probed on 2.4.8 and 3.5.5 at the top of a select and inside when, struct,
    size, to_json, array_contains and array: the same names with and without it.

    The type classes are imported from pyspark.sql.types beside an existing
    import of it, else after the file's opening imports -- unless the file
    already has them (imported by name or by `*`). A file that binds one of
    the names to something else gets `__import__("pyspark.sql.types").sql.types.X`."""

    rule_id = "EmptyCollectionTypeDetect"
    TYPES = {"array": ("ArrayType", "{a}({s}(), False)"), "create_map": ("MapType", "{a}({s}(), {s}(), False)")}
    QUALIFIED = '__import__("pyspark.sql.types").sql.types.'

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        self.coerced: set[int] = set()
        self.imported = set(_cst.names_imported(ctx.module, _cst.TYPES))
        self.to_import: list[str] = []

    def _ref(self, name: str) -> str:
        """How the rewrite names a pyspark.sql.types class, importing it if need be."""
        bound = self.ctx.bindings.count.get(name, 0)
        if name in self.imported and bound == 1 or not bound and self.ctx.star_types:
            return name
        if bound:
            return self.QUALIFIED + name
        if name not in self.to_import:
            self.to_import.append(name)
        return name

    def _empty(self, node: cst.BaseExpression | None) -> bool:
        return isinstance(node, cst.Call) and self.function(node, "array", "create_map") is not None \
            and _collection_args(node) == []

    def visit_Call(self, node: cst.Call) -> None:
        # Parents are visited before their arguments, so a coerced argument is
        # marked before it is reached.
        args = _cst.positional(node)
        if (self.function(node, *_COERCING) or _cst.method_name(node) in _COERCING) and len(args) > 1:
            self.coerced.update(id(a) for a in args if self._empty(a))
        if _cst.method_name(node) == "otherwise":
            branches = args[:1] + [_cst.argument(w, 1, "value") for w in _cst.spine_calls(_cst.receiver(node), "when")]
            self.coerced.update(id(b) for b in branches if self._empty(b))
        # Already cast to a type: `F.array().cast("array<int>")`.
        if _cst.method_name(node) == "cast" and self._empty(_cst.receiver(node)):
            self.coerced.add(id(_cst.receiver(node)))

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.BaseExpression:
        fn = self.function(original_node, "array", "create_map")
        if fn is None or _collection_args(original_node) != [] or id(original_node) in self.coerced:
            return updated_node
        element = "array<void>" if fn == "array" else "map<void,void>"
        outer, shape = self.TYPES[fn]
        to = cst.parse_expression(shape.format(a=self._ref(outer), s=self._ref("StringType")))
        cast = cst.Call(func=cst.Attribute(value=updated_node, attr=cst.Name("cast")), args=[cst.Arg(to)])
        return self.rewrite(original_node, cast,
                            f"{fn}() with no elements is {element} (NullType) from Spark 3.0, not 2.4's string elements -- "
                            "Parquet and ORC cannot write it. Rewritten with the cast to 2.4's type, non-null elements "
                            "included; the alternative, spark.sql.legacy.createEmptyCollectionUsingStringType=true, changes "
                            "every empty collection in the job.")

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        if not self.to_import:
            return updated_node
        body = list(updated_node.body)
        for i, stmt in enumerate(body):
            for small in stmt.body if isinstance(stmt, cst.SimpleStatementLine) else ():
                if isinstance(small, cst.ImportFrom) and small.module is not None and not small.relative and \
                        not isinstance(small.names, cst.ImportStar) and get_full_name_for_node(small.module) == _cst.TYPES:
                    names = list(small.names)
                    names[-1] = names[-1].with_changes(comma=cst.MaybeSentinel.DEFAULT)
                    added = names + [cst.ImportAlias(name=cst.Name(n)) for n in self.to_import]
                    body[i] = stmt.with_changes(body=[small.with_changes(names=added) if s is small else s for s in stmt.body])
                    return updated_node.with_changes(body=body)
        body.insert(_cst.after_leading_imports(body),
                    cst.parse_statement(f"from {_cst.TYPES} import {', '.join(self.to_import)}\n"))
        return updated_node.with_changes(body=body)


class HashOnMapTypeDetect(Detector):
    rule_id = "HashOnMapTypeDetect"

    def visit_Call(self, node: cst.Call) -> None:
        fn = self.function(node, "hash", "xxhash64")
        if fn is None:
            return
        if any(isinstance(a, cst.Call) and self.function(a, "create_map", "map_from_arrays") for a in _cst.positional(node)):
            self.report(node, f"{fn}(...) is applied to a map. Spark 3.0 rejects hashing a MapType at analysis time (equal maps "
                              "can hash differently); 2.4 allowed it. Usually unintended -- hash map_entries() or the values "
                              "instead, or set spark.sql.legacy.allowHashOnMapType=true.")


_SPLIT = ("split(col, \"\") splits on an empty regex. Since Spark 3.4 the result has no trailing empty string "
          "(\"abc\" -> [a, b, c], where 2.4 gave [a, b, c, '']); no config restores it.")


class SplitEmptyRegexDetect(NameDiscarded, Fixer):
    """`split(col, "")`: since 3.4 the result has no trailing empty string.
    Rewritten to `split(col, "(?=)")` -- a zero-width match at every position,
    what 2.4's Java `String.split` did with "" -- probed on 2.4.8 and 3.5.5 for
    "abc", "", None, one character, separators, spaces, accented letters and
    a supplementary character: the same arrays (and 2.4.8 runs it too). The
    rewrite renames an unaliased result column (`split(s, )` on 2.4,
    `split(s, (?=), -1)` on 3.5.5), so it is made only where the generated
    name is discarded; elsewhere, and for a pattern bound to a name, it is
    `SplitEmptyRegexDetect.Manual`. PySpark 2.4's split takes no limit, so
    only the two-argument form is matched."""

    rule_id = "SplitEmptyRegexDetect"
    MANUAL = "SplitEmptyRegexDetect.Manual"

    def visit_Call(self, node: cst.Call) -> None:
        self.mark_unnamed(node)

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.BaseExpression:
        pattern = _cst.argument(original_node, 1, "pattern")
        if not self.function(original_node, "split") or self.string(pattern) != "" or \
                _cst.argument(original_node, 2, "limit") is not None:
            return updated_node
        if not self.unnamed_depth or not isinstance(pattern, cst.SimpleString):
            self.report(original_node, f"{_SPLIT} The rewrite that keeps 2.4's result, split(col, \"(?=)\"), would rename "
                                       "the result column, so it is not made here. Use it with .alias(...) of the current "
                                       "name if anything reads the column by name.", tier=3, rule_id=self.MANUAL)
            return updated_node
        quote = len(pattern.prefix)
        half = quote + (len(pattern.value) - quote) // 2
        new = pattern.with_changes(value=pattern.value[:half] + "(?=)" + pattern.value[half:])
        at = [a.value for a in original_node.args].index(pattern)
        args = [a.with_changes(value=new) if i == at else a for i, a in enumerate(updated_node.args)]
        return self.rewrite(original_node, updated_node.with_changes(args=args),
                            f"{_SPLIT} Rewritten to split(col, \"(?=)\"), which gives 2.4's result on Spark 3.x too.")


class GroupingIdTypeDetect(Detector):
    rule_id = "GroupingIdTypeDetect"

    def visit_Call(self, node: cst.Call) -> None:
        if self.function(node, "grouping_id"):
            self.report(node, "grouping_id() is a bigint from Spark 3.0, an int in 2.4. Python sees an int either way; what "
                              "changes is the schema -- a column compared with an expected schema, or written to an INT column "
                              "-- and spark.sql.legacy.integerGroupingId=true restores it.")
