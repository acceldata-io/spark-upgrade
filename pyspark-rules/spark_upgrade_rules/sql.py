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

Codegen rewrites only SQL that reaches Spark (the owner, 2026-10-15). A defined
statement says whether it does in this module -- `reaches`: it is written in a
`.sql(...)` argument, or bound once to a name a `.sql(...)` call here uses,
directly or through `.format`/`%` -- and gives that `name`. A call site whose
argument is an imported name gives `ref`, the import it resolves to
(`jobs.queries.DAILY`, `.queries.DAILY`), so the CLI can tell a constant
another module defines reaches Spark.

One helper deep, too: a module-level function that hands one of its parameters
to `.sql(...)` is a `sink` (`def run(spark, sql): return spark.sql(sql)`), and
a call that passes a name into it is a `pass` -- `run(spark, DAILY)`,
`sql_runner.run(spark, QUERIES[name])`. A definition inside a dict or list
gives the `container` it sits in, so passing `QUERIES[name]` reaches every
entry of `QUERIES`. The CLI joins sinks, passes and definitions across files.
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
      | show\s+(?:databases|schemas|tables|table\s+extended)\b
      | desc(?:ribe)?\s+(?:database|schema)\b
      | set\s+[\w.]+\s*=
    )""",
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)
PARAM = "__param__"
_FIELD = re.compile(r"\{[A-Za-z_]\w*\}")
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
        consumed = calls.raw
    found = _SqlDefinitions(consumed)
    module.visit(found)
    return found.found


def _root(node: cst.BaseExpression) -> cst.BaseExpression:
    """What a template call is built from: `t.format(...)` and `t % args` are `t`."""
    while True:
        if isinstance(node, cst.Call) and isinstance(node.func, cst.Attribute) and node.func.attr.value == "format":
            node = node.func.value
        elif isinstance(node, cst.BinaryOperation) and isinstance(node.operator, cst.Modulo):
            node = node.left
        else:
            return node


def _dotted(node: cst.BaseExpression) -> str | None:
    if isinstance(node, cst.Name):
        return node.value
    if isinstance(node, cst.Attribute):
        inner = _dotted(node.value)
        return inner + "." + node.attr.value if inner else None
    return None


class _Imports(cst.CSTVisitor):
    """Each imported name, as the dotted path it imports: `from .q import A as B`
    binds B to `.q.A`; `import a.b as c` binds c to `a.b`, `import a.b` a to `a`."""

    def __init__(self) -> None:
        self.paths: dict[str, str] = {}

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        if isinstance(node.names, cst.ImportStar):
            return
        module = "." * len(node.relative) + (_dotted(node.module) or "")
        for alias in node.names:
            name = _dotted(alias.name)
            bound = alias.asname.name.value if alias.asname else name
            self.paths[bound] = (module + "." if module and not module.endswith(".") else module) + name

    def visit_Import(self, node: cst.Import) -> None:
        for alias in node.names:
            name = _dotted(alias.name)
            if alias.asname:
                self.paths[alias.asname.name.value] = name
            else:
                self.paths[name.split(".")[0]] = name.split(".")[0]

    def resolve(self, dotted: str | None) -> str | None:
        if not dotted:
            return None
        head = dotted.split(".")[0]
        return self.paths[head] + dotted[len(head):] if head in self.paths else None


class _Ids(cst.CSTVisitor):
    def __init__(self) -> None:
        self.ids: set[int] = set()

    def on_visit(self, node: cst.CSTNode) -> bool:
        self.ids.add(id(node))
        return True


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

    def __init__(self, bindings: Bindings, imports: _Imports | None = None) -> None:
        self.bindings = bindings
        self.imports = imports or _Imports()
        self.found: list[dict] = []
        # What reaches a `.sql(...)` call: every node of its argument, and the
        # names its template is built from.
        self.at_call: set[int] = set()
        self.names: set[str] = set()
        # Each literal call site's text as written, before its fields are
        # filled: what a definition is matched against, so it is not linted twice.
        self.raw: set[str] = set()

    def visit_Call(self, node: cst.Call) -> None:
        if not (isinstance(node.func, cst.Attribute) and node.func.attr.value == "sql"):
            return
        positional = [a for a in node.args if a.keyword is None and not a.star]
        if not positional:
            return
        arg = positional[0].value
        line = self.get_metadata(PositionProvider, node).start.line
        ids = _Ids()
        arg.visit(ids)
        self.at_call |= ids.ids
        root = _root(arg)
        if isinstance(root, cst.Subscript):  # QUERIES[name]: every entry of QUERIES
            root = root.value
        if isinstance(root, cst.Name):
            self.names.add(root.value)
        ref = self.imports.resolve(_dotted(root))

        text = _string_value(arg)
        if text is None and isinstance(arg, cst.Name):
            text = self.bindings.resolve(arg.value)
        if text is not None:
            # `{name}` is a formatter field -- pyspark.pandas.sql always fills it,
            # spark.sql since 3.4 when the names are passed -- so it is linted
            # filled, like a template's placeholder.
            self.raw.add(text)
            text = _FIELD.sub(PARAM, text)
            self.found.append({"line": line, "kind": LITERAL, "text": text})
        else:
            site = {"line": line, "kind": INTERPOLATED if _is_interpolated(arg) else DYNAMIC}
            if ref:
                site["ref"] = ref
            # What `extract_sql` needs to tell whether this site's SQL was read
            # where it is defined; removed before the site is returned.
            site["_ids"] = ids.ids
            site["_name"] = root.value if isinstance(root, cst.Name) else ""
            self.found.append(site)


class _Helpers(cst.CSTVisitor):
    """Sinks -- module-level functions whose parameter reaches `.sql(...)` --
    and passes -- calls that hand a name, or an item of one, to a function."""

    METADATA_DEPENDENCIES = (PositionProvider,)

    def __init__(self, imports: _Imports) -> None:
        self.imports = imports
        self.found: list[dict] = []
        self.depth = 0

    def visit_FunctionDef(self, node: cst.FunctionDef) -> None:
        if self.depth == 0:
            params = [p.name.value for p in node.params.params]
            line = self.get_metadata(PositionProvider, node).start.line
            for index, param in self._sunk(node, params):
                self.found.append({"line": line, "kind": "sink", "name": node.name.value, "index": index, "keyword": param})
        self.depth += 1

    def leave_FunctionDef(self, node: cst.FunctionDef) -> None:
        self.depth -= 1

    def visit_ClassDef(self, node: cst.ClassDef) -> None:
        self.depth += 1

    def leave_ClassDef(self, node: cst.ClassDef) -> None:
        self.depth -= 1

    @staticmethod
    def _sunk(node: cst.FunctionDef, params: list[str]) -> list[tuple[int, str]]:
        found: set[str] = set()

        class Calls(cst.CSTVisitor):
            def visit_Call(self, call: cst.Call) -> None:
                if isinstance(call.func, cst.Attribute) and call.func.attr.value == "sql" and call.args:
                    root = _root(call.args[0].value)
                    if isinstance(root, cst.Name) and root.value in params:
                        found.add(root.value)

        node.body.visit(Calls())
        return [(i, p) for i, p in enumerate(params) if p in found]

    def visit_Call(self, node: cst.Call) -> None:
        if isinstance(node.func, cst.Attribute) and node.func.attr.value == "sql":
            return
        callee = _dotted(node.func)
        if not callee:
            return
        resolved = self.imports.resolve(callee)
        line = self.get_metadata(PositionProvider, node).start.line
        index = 0
        for a in node.args:
            if a.star:
                continue
            value = _root(a.value)
            if isinstance(value, cst.Subscript):
                value = value.value
            arg = _dotted(value)
            if arg:
                site = {"line": line, "kind": "pass", "callee": resolved or callee, "index": index if a.keyword is None else -1,
                        "keyword": a.keyword.value if a.keyword is not None else ""}
                arg_ref = self.imports.resolve(arg)
                if arg_ref:
                    site["ref"] = arg_ref
                elif "." not in arg:
                    site["name"] = arg
                if "ref" in site or "name" in site:
                    self.found.append(site)
            if a.keyword is None:
                index += 1


def _containers(module: cst.Module) -> dict[int, str]:
    """Each element of a dict or list bound to a name, to that name."""
    found: dict[int, str] = {}

    class Assigns(cst.CSTVisitor):
        def visit_Assign(self, node: cst.Assign) -> None:
            if len(node.targets) == 1 and isinstance(node.targets[0].target, cst.Name):
                name = node.targets[0].target.value
                if isinstance(node.value, cst.Dict):
                    found.update((id(e.value), name) for e in node.value.elements if isinstance(e, cst.DictElement))
                elif isinstance(node.value, (cst.List, cst.Tuple)):
                    found.update((id(e.value), name) for e in node.value.elements)

    module.visit(Assigns())
    return found


def extract_sql(module: cst.Module) -> list[dict]:
    """Every `.sql(...)` call site in `module`, in source order, as
    `{"line": int, "kind": "literal"|"interpolated"|"dynamic", "text": str, "ref": str, "read": bool}`
    -- `text` only for literals, `ref` only for an imported name, `read` when the
    site's SQL is linted where it is defined in this module -- then every
    defined statement, as
    `{"line": int, "kind": "defined", "text": str, "name": str, "container": str, "reaches": bool}`,
    then every sink, `{"line", "kind": "sink", "name": function, "index": int, "keyword": parameter}`,
    and pass, `{"line", "kind": "pass", "callee": str, "index": int (-1 for a keyword argument),
    "keyword": str, "name" | "ref": str}`."""
    wrapper = MetadataWrapper(module, unsafe_skip_copy=True)
    bindings = bindings_of(module)
    imports = _Imports()
    module.visit(imports)
    calls = _SqlCalls(bindings, imports)
    wrapper.visit(calls)
    positions = wrapper.resolve(PositionProvider)
    consumed = calls.raw
    bound = {id(v): k for k, v in bindings.value.items() if bindings.count.get(k) == 1}
    containers = _containers(module)
    defined = []
    for node, text in sql_definitions(module, consumed):
        name = bound.get(id(node), "")
        container = containers.get(id(node), "")
        defined.append({"line": positions[node].start.line, "kind": DEFINED, "text": text, "name": name,
                        "container": container,
                        "reaches": id(node) in calls.at_call or name in calls.names or container in calls.names})
    # A call site whose SQL was linted where it is defined -- an f-string or a
    # template written in its argument, or a name bound in this file to a
    # statement or to a registry of them -- is `read`, not a call site for a
    # person. An imported name is resolved by the CLI (`ref`).
    read_nodes = {id(node) for node, _ in sql_definitions(module, consumed)}
    read_names = {d["name"] for d in defined if d["name"]} | {d["container"] for d in defined if d["container"]}
    for site in calls.found:
        ids, name = site.pop("_ids", set()), site.pop("_name", "")
        if site["kind"] in (INTERPOLATED, DYNAMIC) and (ids & read_nodes or name in read_names):
            site["read"] = True
    helpers = _Helpers(imports)
    wrapper.visit(helpers)
    return calls.found + defined + helpers.found
