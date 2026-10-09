"""Family G: DataFrame API behaviour changes -- the ambiguous self-join and the
DataFrameNaFunctions column-name change. Matched on method shape (a DataFrame
has no qualified name), narrowed by what the probes against Spark 3.5.5
showed actually fails (PYSPARK.md SS8.12).
"""
from __future__ import annotations

import libcst as cst

from spark_upgrade_rules import _cst
from spark_upgrade_rules.detector import Detector


class _Scopes(cst.CSTVisitor):
    """Names assigned exactly once per function (or module) scope, with the
    name at the root of what they were assigned -- `df2 = df.filter(...)` ->
    df2: df. One hop, by design."""

    def __init__(self) -> None:
        self.stack: list[dict[str, list]] = [{}]
        self.by_scope: dict[cst.CSTNode, dict[str, list]] = {}

    def visit_FunctionDef(self, node: cst.FunctionDef) -> None:
        self.stack.append({})
        self.by_scope[node] = self.stack[-1]

    def leave_FunctionDef(self, original_node: cst.FunctionDef) -> None:
        self.stack.pop()

    def visit_Assign(self, node: cst.Assign) -> None:
        for t in node.targets:
            if isinstance(t.target, cst.Name):
                self.stack[-1].setdefault(t.target.value, []).append(node.value)


def _root_name(node: cst.BaseExpression | None) -> str | None:
    root = _cst.spine_root(node)
    return root.value if isinstance(root, cst.Name) else None


def _aliased(node: cst.BaseExpression | None) -> bool:
    return bool(_cst.spine_calls(node, "alias"))


class SelfJoinAmbiguousColumnDetect(Detector):
    """A join of two DataFrames that are the same frame or one derived from the
    other in one step (`df2 = df.filter(...)`), neither aliased, where a column
    is then referenced through either frame (`df.a`, `df2["a"]`) other than as a
    same-name `==`/`!=` pair. Probed on 3.5.5: `df.join(df2, df.a > df2.a)`,
    `df.a == df2.b` and `.select(df2.b)` after the join all throw "Column ... are
    ambiguous"; `df.join(df2, "a")`, `df.a == df2.a` and a `.drop(df2.b)` after
    the join do not. The Scala rule's
    `df.join(df, ...)` shape is only a hit here when its condition is one of the
    failing ones."""

    rule_id = "SelfJoinAmbiguousColumnDetect"

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        scopes = _Scopes()
        ctx.module.visit(scopes)
        self.module_scope = scopes.stack[0]
        self.by_scope = scopes.by_scope
        self.scope: list[dict[str, list]] = [self.module_scope]
        self.reported: set[int] = set()

    def visit_FunctionDef(self, node: cst.FunctionDef) -> None:
        self.scope.append(self.by_scope.get(node, {}))

    def leave_FunctionDef(self, original_node: cst.FunctionDef) -> None:
        self.scope.pop()

    def _derived_from(self, name: str) -> str | None:
        for scope in reversed(self.scope):
            if name in scope:
                values = scope[name]
                return _root_name(values[0]) if len(values) == 1 and not _aliased(values[0]) else None
        return None

    def _same_lineage(self, a: str, b: str) -> bool:
        return a == b or self._derived_from(b) == a or self._derived_from(a) == b

    def _sides(self, join: cst.Call) -> tuple[str, str] | None:
        recv, other = _cst.receiver(join), _cst.argument(join, 0, "other")
        if not (isinstance(recv, cst.Name) and isinstance(other, cst.Name)):
            return None
        return (recv.value, other.value) if self._same_lineage(recv.value, other.value) else None

    @staticmethod
    def _column_ref(node: cst.BaseExpression, names: tuple[str, str]) -> tuple[str, str] | None:
        """(frame, column) for `df.col` or `df["col"]` on one of `names`."""
        if isinstance(node, cst.Attribute) and isinstance(node.value, cst.Name) and node.value.value in names:
            return node.value.value, node.attr.value
        if isinstance(node, cst.Subscript) and isinstance(node.value, cst.Name) and node.value.value in names \
                and len(node.slice) == 1 and isinstance(node.slice[0].slice, cst.Index):
            col = _cst.string_value(node.slice[0].slice.value)
            if col is not None:
                return node.value.value, col
        return None

    def _risky_refs(self, node: cst.CSTNode, names: tuple[str, str]) -> list[str]:
        """Column references through either frame, minus same-name `==`/`!=`
        pairs, which Spark resolves."""
        out: list[str] = []
        stack = [node]
        while stack:
            n = stack.pop()
            if isinstance(n, cst.Comparison) and len(n.comparisons) == 1 and \
                    isinstance(n.comparisons[0].operator, (cst.Equal, cst.NotEqual)):
                left = self._column_ref(n.left, names)
                right = self._column_ref(n.comparisons[0].comparator, names)
                if left and right and left[1] == right[1]:
                    continue
            if isinstance(n, cst.BaseExpression):
                ref = self._column_ref(n, names)
                if ref is not None:
                    out.append(f"{ref[0]}.{ref[1]}")
                    continue
            stack.extend(n.children)
        return out

    def visit_Call(self, node: cst.Call) -> None:
        if _cst.method_name(node) == "join":
            sides = self._sides(node)
            if sides is not None:
                on = _cst.argument(node, 1, "on")
                refs = self._risky_refs(on, sides) if on is not None else []
                if refs:
                    self._flag(node, sides, refs[0], "in the join condition")
        # A column of either frame referenced further down the chain, after the
        # join -- except in .drop(...), which 3.5.5 resolves without the
        # ambiguity check (probed: `.drop(df.c)` after the join runs, the same
        # `.select(df.c)` throws).
        if _cst.method_name(node) == "drop":
            return
        for inner in _cst.spine(_cst.receiver(node)):
            if isinstance(inner, cst.Call) and _cst.method_name(inner) == "join":
                sides = self._sides(inner)
                if sides is not None:
                    refs = [r for a in node.args for r in self._risky_refs(a.value, sides)]
                    if refs:
                        self._flag(inner, sides, refs[0], f"in the .{_cst.method_name(node)}(...) after the join")
                break

    def _flag(self, join: cst.Call, sides: tuple[str, str], ref: str, where: str) -> None:
        if id(join) in self.reported:
            return
        self.reported.add(id(join))
        how = "itself" if sides[0] == sides[1] else f"{sides[1]}, which is derived from it" \
            if self._derived_from(sides[1]) == sides[0] else f"{sides[1]}, which it is derived from"
        self.report(join, f"{sides[0]} is joined to {how}, neither side aliased, and {ref} is referenced {where}. Spark 3.0 "
                          "fails this as an ambiguous self-join (\"Column ... are ambiguous\"); 2.4 resolved it silently, often "
                          "to the wrong side. Alias both sides and use F.col('a.x'), or set "
                          "spark.sql.analyzer.failAmbiguousSelfJoin=false.")


class NaFunctionsNameMatchDetect(Detector):
    """`fillna`/`na.fill` and `replace`/`na.replace` given column names. Since
    3.2 the names resolve like SQL: a missing column throws AnalysisException
    (2.4 ignored it), a dotted name needs backticks, a nested one throws.
    Probed on 3.5.5. Calls with no column names are unaffected and not
    flagged -- unlike the Scala rule, which flags every call. pandas also has
    `fillna`/`replace`, so the bare methods need a `subset`."""

    rule_id = "NaFunctionsNameMatchDetect"

    def visit_Call(self, node: cst.Call) -> None:
        name = _cst.method_name(node)
        via_na = isinstance(_cst.receiver(node), cst.Attribute) and _cst.receiver(node).attr.value == "na"
        if name in ("fill", "fillna") and (via_na or name == "fillna"):
            subset = _cst.argument(node, 1, "subset") if via_na else _cst.keyword(node, "subset") or self._list_arg(node, 1)
            value = _cst.argument(node, 0, "value")
            names = self._names(subset) + ([k for k, _ in _cst.dict_items(value)] if via_na else [])
            if subset is not None or names:
                self._flag(node, f"{'na.' if via_na else ''}{name}", names)
        elif name == "replace" and (via_na or _cst.keyword(node, "subset") is not None or self._list_arg(node, 2) is not None):
            subset = _cst.argument(node, 2, "subset") if via_na else _cst.keyword(node, "subset") or self._list_arg(node, 2)
            if subset is not None:
                self._flag(node, f"{'na.' if via_na else ''}replace", self._names(subset))

    @staticmethod
    def _list_arg(node: cst.Call, index: int) -> cst.BaseExpression | None:
        args = _cst.positional(node)
        return args[index] if index < len(args) and isinstance(args[index], (cst.List, cst.Tuple)) else None

    def _names(self, subset: cst.BaseExpression | None) -> list[str]:
        if subset is None or _cst.literal_value(subset) == ("const", "None"):
            return []
        single = self.string(subset)
        if single is not None:
            return [single]
        if isinstance(subset, (cst.List, cst.Tuple)):
            return [s for s in (_cst.string_value(el.value) for el in subset.elements) if s is not None]
        return []

    def _flag(self, node: cst.Call, call: str, names: list[str]) -> None:
        dotted = [n for n in names if "." in n and not n.startswith("`")]
        if dotted:
            self.report(node, f"{call}(...) names the column {dotted[0]!r}. Since Spark 3.2 column names here resolve like SQL, "
                              f"so a dot means a nested field: write `{dotted[0]}` in backticks, or the call throws.", "high")
        else:
            self.report(node, f"{call}(...) is given column names. Since Spark 3.2 they resolve like SQL: a name that is not in "
                              "the frame throws AnalysisException (2.4 skipped it silently), and a nested name throws. No "
                              "config restores 2.4; check every name exists.")
