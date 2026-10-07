"""Embedded SQL extraction -- plumbing, not a rule.

Finds every `<expr>.sql(<arg>, ...)` call site and classifies its first
argument the way the Scala path's SqlInStringDetect does:

  literal       a string literal, an implicit concatenation of them, or a name
                bound exactly once in the module to one of those
  interpolated  an f-string, `"..." % args`, or `"...".format(...)`
  dynamic       anything else -- a variable, a function call, `+` concatenation

The text of each literal goes back to spark-migrate-cli, which lints it with
the same 13-code sqlfluff allowlist and regex detectors the Scala and Java
paths use. Interpolated and dynamic SQL cannot be linted; they are reported as
call sites a person has to read.

The matcher is the same `.sql(...)` attribute-call shape PySparkler's own
PY21-33-001 uses: `spark.sql`, `sqlContext.sql`, `self.spark.sql` alike.
"""
from __future__ import annotations

import libcst as cst
from libcst.metadata import MetadataWrapper, PositionProvider

LITERAL = "literal"
INTERPOLATED = "interpolated"
DYNAMIC = "dynamic"


def _string_value(node: cst.BaseExpression) -> str | None:
    """The value of a plain string literal, or None. Bytes and f-strings are not plain."""
    if isinstance(node, (cst.SimpleString, cst.ConcatenatedString)):
        try:
            value = node.evaluated_value
        except Exception:  # pylint: disable=broad-except
            return None
        return value if isinstance(value, str) else None
    return None


def _is_interpolated(node: cst.BaseExpression) -> bool:
    if isinstance(node, cst.FormattedString):
        return True
    if isinstance(node, cst.ConcatenatedString):
        return _is_interpolated(node.left) or _is_interpolated(node.right)
    if isinstance(node, cst.BinaryOperation) and isinstance(node.operator, cst.Modulo):
        return _string_value(node.left) is not None or _is_interpolated(node.left)
    if (
        isinstance(node, cst.Call)
        and isinstance(node.func, cst.Attribute)
        and node.func.attr.value == "format"
    ):
        return _string_value(node.func.value) is not None
    return False


class _Bindings(cst.CSTVisitor):
    """Every name bound anywhere in the module, and the string literal it was
    bound to when that is its only binding. One hop, no flow analysis: a name
    assigned twice, or ever assigned anything else, is not resolved."""

    def __init__(self) -> None:
        self.count: dict[str, int] = {}
        self.literal: dict[str, str] = {}

    def _bind(self, name: str, value: cst.BaseExpression | None) -> None:
        self.count[name] = self.count.get(name, 0) + 1
        text = _string_value(value) if value is not None else None
        if text is not None:
            self.literal[name] = text

    def visit_Assign(self, node: cst.Assign) -> None:
        for target in node.targets:
            if isinstance(target.target, cst.Name):
                self._bind(target.target.value, node.value if len(node.targets) == 1 else None)
            else:
                for name in _names_in(target.target):
                    self._bind(name, None)

    def visit_AnnAssign(self, node: cst.AnnAssign) -> None:
        if isinstance(node.target, cst.Name):
            self._bind(node.target.value, node.value)

    def visit_AugAssign(self, node: cst.AugAssign) -> None:
        if isinstance(node.target, cst.Name):
            self._bind(node.target.value, None)

    def visit_For(self, node: cst.For) -> None:
        for name in _names_in(node.target):
            self._bind(name, None)

    def visit_Param(self, node: cst.Param) -> None:
        self._bind(node.name.value, None)

    def resolve(self, name: str) -> str | None:
        return self.literal.get(name) if self.count.get(name) == 1 else None


def _names_in(node: cst.BaseExpression) -> list[str]:
    if isinstance(node, cst.Name):
        return [node.value]
    if isinstance(node, (cst.Tuple, cst.List)):
        return [n for el in node.elements for n in _names_in(el.value)]
    return []


class _SqlCalls(cst.CSTVisitor):
    METADATA_DEPENDENCIES = (PositionProvider,)

    def __init__(self, bindings: _Bindings) -> None:
        self.bindings = bindings
        self.found: list[dict] = []

    def visit_Call(self, node: cst.Call) -> None:
        if not (isinstance(node.func, cst.Attribute) and node.func.attr.value == "sql"):
            return
        positional = [a for a in node.args if a.keyword is None and not a.star]
        if not positional:
            return
        arg = positional[0].value
        line = self.get_metadata(PositionProvider, node).start.line

        text = _string_value(arg)
        if text is None and isinstance(arg, cst.Name):
            text = self.bindings.resolve(arg.value)
        if text is not None:
            self.found.append({"line": line, "kind": LITERAL, "text": text})
        elif _is_interpolated(arg):
            self.found.append({"line": line, "kind": INTERPOLATED})
        else:
            self.found.append({"line": line, "kind": DYNAMIC})


def extract_sql(module: cst.Module) -> list[dict]:
    """Every `.sql(...)` call site in `module`, in source order, as
    `{"line": int, "kind": "literal"|"interpolated"|"dynamic", "text": str}`
    -- `text` only for literals."""
    bindings = _Bindings()
    module.visit(bindings)
    calls = _SqlCalls(bindings)
    MetadataWrapper(module, unsafe_skip_copy=True).visit(calls)
    return calls.found
