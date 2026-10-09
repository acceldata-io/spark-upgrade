"""Families E and F, and grouping_id: built-in functions and types whose
behaviour changed for a literal-decidable argument shape. Ports of the Scala
rules of the same ids; each narrows to the shape that is certain from the
call alone, as the Scala rule does.
"""
from __future__ import annotations

import libcst as cst

from spark_upgrade_rules import _cst
from spark_upgrade_rules.detector import Detector, Fixer


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
    string in 2.4. Rewritten to `.cast("array<string>")` /
    `.cast("map<string,string>")` -- exactly 2.4's type, at the call alone,
    where the legacy config would change every empty collection in the job.
    Probed on 3.5.5: the cast array writes to Parquet, the bare one does not."""

    rule_id = "EmptyCollectionTypeDetect"
    CASTS = {"array": "array<string>", "create_map": "map<string,string>"}

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        self.coerced: set[int] = set()

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
        cast = cst.Call(func=cst.Attribute(value=updated_node, attr=cst.Name("cast")),
                        args=[cst.Arg(cst.SimpleString(f'"{self.CASTS[fn]}"'))])
        return self.rewrite(original_node, cast,
                            f"{fn}() with no elements is {element} (NullType) from Spark 3.0, not 2.4's string elements -- "
                            "Parquet and ORC cannot write it. Rewritten with the cast to 2.4's type; the alternative, "
                            "spark.sql.legacy.createEmptyCollectionUsingStringType=true, changes every empty collection in the job.")


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


class SplitEmptyRegexDetect(Detector):
    rule_id = "SplitEmptyRegexDetect"

    def visit_Call(self, node: cst.Call) -> None:
        if self.function(node, "split") and self.string(_cst.argument(node, 1, "pattern")) == "":
            self.report(node, 'split(col, "") splits on an empty regex. Since Spark 3.4 the result has no trailing empty string '
                              "(\"abc\" -> [a, b, c], where 2.4 gave [a, b, c, '']). No config restores it; check code that "
                              "indexes or counts the result.")


class GroupingIdTypeDetect(Detector):
    rule_id = "GroupingIdTypeDetect"

    def visit_Call(self, node: cst.Call) -> None:
        if self.function(node, "grouping_id"):
            self.report(node, "grouping_id() is a bigint from Spark 3.0, an int in 2.4. Python sees an int either way; what "
                              "changes is the schema -- a column compared with an expected schema, or written to an INT column "
                              "-- and spark.sql.legacy.integerGroupingId=true restores it.")
