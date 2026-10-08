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

A call site sees only the SQL written into it. Real SQL-heavy code mostly
keeps its statements elsewhere -- a queries module whose constants another
file runs, a dict of named queries, a DDL list run in a loop, a `.format` or
`string.Template` template, an f-string -- and every one of those reaches the
call as a name, a subscript or a call: dynamic. So SQL is also found where it
is *defined*:

  defined       a string literal (or implicit concatenation, or f-string)
                whose text is a SQL statement by its shape, wherever it is
                written, unless it is already the literal of a call site in
                the same file. Template placeholders ({x}, {}, %s, %(x)s,
                $x, ${x}) and f-string expressions become `__param__`, so the
                rest of the statement is linted.

Docstrings, and strings handed to logging, print or an exception, are never
SQL. A defined statement sqlfluff cannot parse is not reported -- it may be a
fragment or prose that happens to start like SQL -- so only rules that fire
on it produce findings.
"""
from __future__ import annotations

import re

import libcst as cst
from libcst.metadata import MetadataWrapper, PositionProvider

from spark_upgrade_rules._cst import Bindings, bindings_of
from spark_upgrade_rules._cst import string_value as _string_value

LITERAL = "literal"
INTERPOLATED = "interpolated"
DYNAMIC = "dynamic"
DEFINED = "defined"

# A SQL statement by its first words -- strict enough that prose ("select the
# file from disk" aside) does not qualify: each verb needs its object.
_STATEMENT = re.compile(
    r"""^\s*(?:(?:--[^\n]*(?:\n|$)|/\*.*?\*/)\s*)*(?:
        select\b.+?\bfrom\b
      | with\s+\w+\s+as\s*\(
      | insert\s+(?:into|overwrite)\b
      | create\s+(?:or\s+replace\s+)?(?:global\s+)?(?:temp(?:orary)?\s+)?(?:external\s+)?
        (?:table|view|database|schema|function)\b
      | alter\s+(?:table|view|database|schema)\b
      | drop\s+(?:table|view|database|schema|function)\b
      | merge\s+into\b
      | from\s+[\w.`]+(?:\s+\w+)?\s*(?:$|select\b|insert\b)
      | (?:truncate|analyze|refresh|uncache)\s+table\b
      | cache\s+(?:lazy\s+)?table\b
      | msck\s+repair\b
      | set\s+[\w.]+\s*=
    )""",
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)
PARAM = "__param__"
_PLACEHOLDER = re.compile(r"\{[^{}\s]*\}|%\([A-Za-z_]\w*\)[sd]|%[sd]|(?<![%\d])\$\{?[A-Za-z_]\w*\}?")
_NOT_SQL_CALLS = {"debug", "info", "warning", "warn", "error", "exception", "critical", "log", "print"}
_NOT_SQL_KEYWORDS = {"help", "description", "doc", "msg", "message", "epilog", "usage"}


def is_statement(text: str) -> bool:
    return _STATEMENT.match(text) is not None


def _template_text(node: cst.BaseExpression) -> str | None:
    """The text of a string literal, with every placeholder as `__param__`."""
    if isinstance(node, cst.SimpleString):
        value = node.evaluated_value
        return _PLACEHOLDER.sub(PARAM, value) if isinstance(value, str) else None
    if isinstance(node, cst.ConcatenatedString):
        left, right = _template_text(node.left), _template_text(node.right)
        return left + right if left is not None and right is not None else None
    if isinstance(node, cst.FormattedString):
        parts = [p.value if isinstance(p, cst.FormattedStringText) else PARAM for p in node.parts]
        return _PLACEHOLDER.sub(PARAM, "".join(parts))
    return None


class _SqlDefinitions(cst.CSTVisitor):
    """Every SQL-statement string literal not already a call site's literal."""

    def __init__(self, consumed: set[str]) -> None:
        self.consumed = consumed
        self.skip: set[int] = set()
        self.found: list[tuple[cst.BaseExpression, str]] = []

    def visit_Expr(self, node: cst.Expr) -> None:
        self.skip.add(id(node.value))  # a docstring, or a bare string statement: never run

    def visit_Call(self, node: cst.Call) -> None:
        name = node.func.attr.value if isinstance(node.func, cst.Attribute) else \
            node.func.value if isinstance(node.func, cst.Name) else ""
        if name in _NOT_SQL_CALLS or name.endswith(("Error", "Exception")):
            self.skip.update(id(a.value) for a in node.args)
        self.skip.update(id(a.value) for a in node.args if a.keyword is not None and a.keyword.value in _NOT_SQL_KEYWORDS)

    def _string(self, node: cst.BaseExpression) -> bool:
        if id(node) in self.skip:
            return False
        if isinstance(node, cst.ConcatenatedString):  # the whole, not its pieces
            self.skip.update((id(node.left), id(node.right)))
        text = _template_text(node)
        if text is not None and is_statement(text) and _string_value(node) not in self.consumed:
            self.found.append((node, text))
        return False

    def visit_SimpleString(self, node: cst.SimpleString) -> bool:
        return self._string(node)

    def visit_ConcatenatedString(self, node: cst.ConcatenatedString) -> bool:
        self._string(node)
        return True

    def visit_FormattedString(self, node: cst.FormattedString) -> bool:
        return self._string(node)


def sql_definitions(module: cst.Module, consumed: set[str] | None = None) -> list[tuple[cst.BaseExpression, str]]:
    """The defined SQL in `module` (see the module docstring), as (node, text)."""
    if consumed is None:
        calls = _SqlCalls(bindings_of(module))
        MetadataWrapper(module, unsafe_skip_copy=True).visit(calls)
        consumed = {c["text"] for c in calls.found if c["kind"] == LITERAL}
    found = _SqlDefinitions(consumed)
    module.visit(found)
    return found.found


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


class _SqlCalls(cst.CSTVisitor):
    METADATA_DEPENDENCIES = (PositionProvider,)

    def __init__(self, bindings: Bindings) -> None:
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
    -- `text` only for literals -- then every defined statement, as
    `{"line": int, "kind": "defined", "text": str}`."""
    wrapper = MetadataWrapper(module, unsafe_skip_copy=True)
    calls = _SqlCalls(bindings_of(module))
    wrapper.visit(calls)
    positions = wrapper.resolve(PositionProvider)
    consumed = {c["text"] for c in calls.found if c["kind"] == LITERAL}
    defined = [{"line": positions[node].start.line, "kind": DEFINED, "text": text}
               for node, text in sql_definitions(module, consumed)]
    return calls.found + defined
