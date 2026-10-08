"""Shared LibCST helpers for the detectors and the SQL extractor.

Everything here is syntax plus one hop of same-file resolution -- the limit
PYSPARK.md SS11.7 sets: a name bound exactly once in the module to a literal is
resolved, nothing else is. No type inference, no cross-file dataflow.
"""
from __future__ import annotations

from typing import Iterator

import libcst as cst

FUNCTIONS = "pyspark.sql.functions"
TYPES = "pyspark.sql.types"


def string_value(node: cst.BaseExpression | None) -> str | None:
    """The value of a plain string literal, or None. Bytes and f-strings are not plain."""
    if isinstance(node, (cst.SimpleString, cst.ConcatenatedString)):
        try:
            value = node.evaluated_value
        except Exception:  # pylint: disable=broad-except
            return None
        return value if isinstance(value, str) else None
    return None


def int_value(node: cst.BaseExpression | None) -> int | None:
    """The value of an integer literal, with a leading unary minus folded in."""
    if isinstance(node, cst.Integer):
        return int(node.evaluated_value)
    if isinstance(node, cst.UnaryOperation) and isinstance(node.operator, cst.Minus):
        inner = int_value(node.expression)
        return -inner if inner is not None else None
    return None


def literal_value(node: cst.BaseExpression | None) -> tuple | None:
    """A hashable stand-in for a Python literal (str, int, float, bool, None),
    tagged by kind so `1` and `"1"` never compare equal. None if not a literal."""
    s = string_value(node)
    if s is not None:
        return ("str", s)
    i = int_value(node)
    if i is not None:
        return ("int", i)
    if isinstance(node, cst.Float):
        return ("float", float(node.evaluated_value))
    if isinstance(node, cst.Name) and node.value in ("True", "False", "None"):
        return ("const", node.value)
    return None


def is_true(node: cst.BaseExpression | None) -> bool:
    """`True`, or a string spelling of it -- Spark reads option values as strings."""
    if isinstance(node, cst.Name):
        return node.value == "True"
    s = string_value(node)
    return s is not None and s.strip().lower() == "true"


class Bindings(cst.CSTVisitor):
    """Every name bound anywhere in the module, and the literal it was bound to
    when that is its only binding. One hop, no flow analysis: a name assigned
    twice, or ever assigned anything else, is not resolved. `value` keeps the
    bound expression itself, for detectors that look inside it (a schema)."""

    def __init__(self) -> None:
        self.count: dict[str, int] = {}
        self.value: dict[str, cst.BaseExpression] = {}

    def _bind(self, name: str, value: cst.BaseExpression | None) -> None:
        self.count[name] = self.count.get(name, 0) + 1
        if value is not None:
            self.value[name] = value

    def visit_Assign(self, node: cst.Assign) -> None:
        for target in node.targets:
            if isinstance(target.target, cst.Name):
                self._bind(target.target.value, node.value if len(node.targets) == 1 else None)
            else:
                for name in names_in(target.target):
                    self._bind(name, None)

    def visit_AnnAssign(self, node: cst.AnnAssign) -> None:
        if isinstance(node.target, cst.Name):
            self._bind(node.target.value, node.value)

    def visit_AugAssign(self, node: cst.AugAssign) -> None:
        if isinstance(node.target, cst.Name):
            self._bind(node.target.value, None)

    def visit_For(self, node: cst.For) -> None:
        for name in names_in(node.target):
            self._bind(name, None)

    def visit_Param(self, node: cst.Param) -> None:
        self._bind(node.name.value, None)

    def visit_FunctionDef(self, node: cst.FunctionDef) -> None:
        self._bind(node.name.value, None)

    def visit_ClassDef(self, node: cst.ClassDef) -> None:
        self._bind(node.name.value, None)

    def visit_ImportAlias(self, node: cst.ImportAlias) -> None:
        bound = node.asname.name if node.asname else node.name
        while isinstance(bound, cst.Attribute):
            bound = bound.value
        if isinstance(bound, cst.Name):
            self._bind(bound.value, None)

    def expression(self, name: str) -> cst.BaseExpression | None:
        """What `name` is bound to, when it is bound exactly once."""
        return self.value.get(name) if self.count.get(name) == 1 else None

    def resolve(self, name: str) -> str | None:
        """The string literal `name` is bound to, when that is its only binding."""
        return string_value(self.expression(name))


def names_in(node: cst.BaseExpression) -> list[str]:
    if isinstance(node, cst.Name):
        return [node.value]
    if isinstance(node, (cst.Tuple, cst.List)):
        return [n for el in node.elements for n in names_in(el.value)]
    return []


def bindings_of(module: cst.Module) -> Bindings:
    b = Bindings()
    module.visit(b)
    return b


def string_or_bound(node: cst.BaseExpression | None, bindings: Bindings) -> str | None:
    """A string literal, or a name bound once to one."""
    text = string_value(node)
    if text is None and isinstance(node, cst.Name):
        text = bindings.resolve(node.value)
    return text


def positional(call: cst.Call) -> list[cst.BaseExpression]:
    """Positional arguments, stopping at the first `*args` -- nothing after it has a known position."""
    out = []
    for a in call.args:
        if a.keyword is not None or a.star == "**":
            continue
        if a.star == "*":
            break
        out.append(a.value)
    return out


def has_star_args(call: cst.Call) -> bool:
    return any(a.star for a in call.args)


def keyword(call: cst.Call, name: str) -> cst.BaseExpression | None:
    for a in call.args:
        if a.keyword is not None and a.keyword.value == name:
            return a.value
    return None


def argument(call: cst.Call, index: int, name: str) -> cst.BaseExpression | None:
    """The argument at positional `index`, or passed as `name=`."""
    kw = keyword(call, name)
    if kw is not None:
        return kw
    args = positional(call)
    return args[index] if index < len(args) else None


def method_name(call: cst.Call) -> str | None:
    """`x.foo(...)` -> "foo"."""
    return call.func.attr.value if isinstance(call.func, cst.Attribute) else None


def receiver(call: cst.Call) -> cst.BaseExpression | None:
    return call.func.value if isinstance(call.func, cst.Attribute) else None


def spine(node: cst.BaseExpression | None) -> Iterator[cst.BaseExpression]:
    """The fluent chain a call sits on, innermost last: for
    `spark.read.option(a).schema(s).csv(p)` called on `.csv`, yields the
    `.schema(s)` call, the `.option(a)` call, `spark.read`, `spark`. Only the
    receiver spine -- never into an argument, so an unrelated `.schema(...)`
    nested inside some option value is not mistaken for the reader's own."""
    while node is not None:
        yield node
        if isinstance(node, cst.Call):
            node = node.func.value if isinstance(node.func, cst.Attribute) else None
        elif isinstance(node, cst.Attribute):
            node = node.value
        elif isinstance(node, cst.Subscript):
            node = node.value
        else:
            node = None


def spine_calls(node: cst.BaseExpression | None, name: str) -> list[cst.Call]:
    """Every `.name(...)` call on the receiver spine of `node`."""
    return [n for n in spine(node) if isinstance(n, cst.Call) and method_name(n) == name]


def spine_root(node: cst.BaseExpression | None) -> cst.BaseExpression | None:
    last = None
    for n in spine(node):
        last = n
    return last


def spine_attrs(node: cst.BaseExpression | None) -> set[str]:
    """Every attribute name on the spine, calls included: {"read", "option", "csv"}."""
    out = set()
    for n in spine(node):
        if isinstance(n, cst.Attribute):
            out.add(n.attr.value)
        elif isinstance(n, cst.Call) and isinstance(n.func, cst.Attribute):
            out.add(n.func.attr.value)
    return out


def dict_items(node: cst.BaseExpression | None) -> list[tuple[str, cst.BaseExpression]]:
    """`{"k": v, ...}` with string keys, or `dict(k=v)`."""
    out = []
    if isinstance(node, cst.Dict):
        for el in node.elements:
            if isinstance(el, cst.DictElement):
                k = string_value(el.key)
                if k is not None:
                    out.append((k, el.value))
    elif isinstance(node, cst.Call) and isinstance(node.func, cst.Name) and node.func.value == "dict":
        out.extend((a.keyword.value, a.value) for a in node.args if a.keyword is not None)
    return out


def options_set(call: cst.Call, bindings: Bindings, own_keywords: bool = True) -> dict[str, cst.BaseExpression]:
    """Reader/writer options set on `call` and its spine, keys lower-cased the
    way Spark's CaseInsensitiveMap reads them: `.option(k, v)`, `.options(k=v)`,
    `.options(**{...})`, `.options({...})`, and -- unless `own_keywords` is
    False -- the keyword arguments of the terminal call itself
    (`.csv(path, multiLine=True)`). A `**{...}` on the terminal call always counts."""
    found: dict[str, cst.BaseExpression] = {}
    for node in spine(call):
        if not isinstance(node, cst.Call):
            continue
        name = method_name(node)
        if name == "option":
            key = string_or_bound(argument(node, 0, "key"), bindings)
            value = argument(node, 1, "value")
            if key is not None and value is not None:
                found.setdefault(key.lower(), value)
        elif name == "options" or node is call:
            for a in node.args:
                if a.keyword is not None:
                    if name == "options" or own_keywords:
                        found.setdefault(a.keyword.value.lower(), a.value)
                elif a.star == "**" or (name == "options" and not a.star):
                    target = a.value
                    if isinstance(target, cst.Name):
                        target = bindings.expression(target.value) or target
                    for k, v in dict_items(target):
                        found.setdefault(k.lower(), v)
    return found


def iter_strings(node: cst.CSTNode) -> Iterator[str]:
    """Every plain string literal anywhere under `node`."""
    stack = [node]
    while stack:
        n = stack.pop()
        s = string_value(n) if isinstance(n, cst.BaseExpression) else None
        if s is not None:
            yield s
            continue
        stack.extend(n.children)
