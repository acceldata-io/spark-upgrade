"""Standard-library names a Spark 2 job's Python had and Spark 3.5.5's does not.

A Spark 2 job usually ran on Python 3.6 or 3.7; Spark 3.5.5 needs 3.8 to 3.11.
Python removed these in that range, and code using them fails at import or at
the call:

  - Rewritten, because the replacement is the same object on every Python 3
    Spark 3.5.5 supports: the `collections` ABC aliases (removed in 3.10) to
    `collections.abc`, `base64.encodestring`/`decodestring` (3.9) to
    `encodebytes`/`decodebytes`, `xml.etree.cElementTree` (3.9) to
    `xml.etree.ElementTree`.
  - Reported, because the replacement behaves differently: `time.clock`,
    `cgi.escape`, `platform.linux_distribution`/`dist` (3.8), `os.errno` (3.7),
    `fractions.gcd` (3.9), `inspect.getargspec` (3.11).
"""
from __future__ import annotations

import libcst as cst
from libcst.helpers import get_full_name_for_node

from spark_upgrade_rules import _cst
from spark_upgrade_rules.detector import Fixer

ABCS = frozenset("""
AsyncGenerator AsyncIterable AsyncIterator Awaitable ByteString Callable Collection Container Coroutine Generator
Hashable ItemsView Iterable Iterator KeysView Mapping MappingView MutableMapping MutableSequence MutableSet Reversible
Sequence Set Sized ValuesView
""".split())

RENAMED_BASE64 = {"encodestring": "encodebytes", "decodestring": "decodebytes"}

# Removed name -> (the Python that removed it, what to use instead).
REPORTED = {
    "time.clock": ("3.8", "time.perf_counter() for elapsed time, time.process_time() for CPU time"),
    "cgi.escape": ("3.8", "html.escape(s, quote=False), the same escaping"),
    "platform.linux_distribution": ("3.8", "the distro package"),
    "platform.dist": ("3.8", "the distro package"),
    "os.errno": ("3.7", "import errno"),
    "fractions.gcd": ("3.9", "math.gcd (it returns a non-negative result)"),
    "inspect.getargspec": ("3.11", "inspect.getfullargspec or inspect.signature"),
}

ELEMENT_TREE = "xml.etree.cElementTree"


def _always(version: str) -> bool:
    """Removed by 3.8, the oldest Python Spark 3.5.5 supports."""
    return tuple(int(p) for p in version.split(".")) <= (3, 8)


def _removed_in(version: str, it: str = "it") -> str:
    return (f"Python {version} removed {it}, so this fails on every Python Spark 3.5.5 supports" if _always(version)
            else f"Python {version} removed {it}, so this fails if the job runs on {version} or later")


class PythonStdlibRemoved(Fixer):
    """See the module docstring."""

    rule_id = "PythonStdlibRemoved"

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        self.needs_abc_import = False
        self.has_abc_import = False

    def visit_Import(self, node: cst.Import) -> None:
        if any(get_full_name_for_node(a.name) == "collections.abc" for a in node.names):
            self.has_abc_import = True

    def leave_Import(self, original_node: cst.Import, updated_node: cst.Import) -> cst.Import:
        names = list(updated_node.names)
        for i, alias in enumerate(original_node.names):
            if get_full_name_for_node(alias.name) != ELEMENT_TREE:
                continue
            if alias.asname is None:
                self.report(original_node, f"{ELEMENT_TREE} is gone: {_removed_in('3.9')}. Import "
                                           "xml.etree.ElementTree instead, the same module since Python 3.3.", "high")
            else:
                names[i] = names[i].with_changes(name=cst.parse_expression("xml.etree.ElementTree"))
        if names == list(updated_node.names):
            return updated_node
        return self.rewrite(original_node, updated_node.with_changes(names=names),
                            f"{ELEMENT_TREE} is gone: {_removed_in('3.9')}. Rewritten to xml.etree.ElementTree, the "
                            "same module since Python 3.3.")

    def leave_SimpleStatementLine(self, original_node: cst.SimpleStatementLine,
                                  updated_node: cst.SimpleStatementLine):
        imp = original_node.body[0] if len(original_node.body) == 1 else None
        if not (isinstance(imp, cst.ImportFrom) and not imp.relative and imp.module is not None
                and get_full_name_for_node(imp.module) == "collections" and not isinstance(imp.names, cst.ImportStar)):
            return updated_node
        abcs = [a for a in imp.names if get_full_name_for_node(a.name) in ABCS]
        if not abcs:
            return updated_node
        rest = [a for a in imp.names if all(a is not b for b in abcs)]
        message = (f"The collections ABC aliases ({', '.join(get_full_name_for_node(a.name) for a in abcs)}) are gone: "
                   f"{_removed_in('3.10', 'them')}. Rewritten to import them from collections.abc, where they live since "
                   "Python 3.3.")

        def line(module: str, aliases) -> cst.SimpleStatementLine:
            aliases = [a.with_changes(comma=cst.MaybeSentinel.DEFAULT) for a in aliases]
            new = imp.with_changes(module=cst.parse_expression(module), names=aliases, lpar=None, rpar=None)
            return updated_node.with_changes(body=[new])

        if not rest:
            new = line("collections.abc", abcs)
            return self.rewrite(original_node, new, message)
        new = cst.FlattenSentinel([line("collections", rest), line("collections.abc", abcs).with_changes(leading_lines=[])])
        self.report(original_node, message, "high", tier=1, fix=f"{self.code(original_node)} -> "
                    f"{self.code(line('collections', rest))}; {self.code(line('collections.abc', abcs))}")
        return new

    def leave_ImportFrom(self, original_node: cst.ImportFrom, updated_node: cst.ImportFrom) -> cst.ImportFrom:
        if original_node.relative or original_node.module is None or isinstance(original_node.names, cst.ImportStar):
            return updated_node
        module = get_full_name_for_node(original_node.module)
        for alias in original_node.names:
            name = get_full_name_for_node(alias.name)
            if f"{module}.{name}" in REPORTED:
                self._report_removed(original_node, f"{module}.{name}")
        if module == ELEMENT_TREE:
            return self.rewrite(original_node, updated_node.with_changes(module=cst.parse_expression("xml.etree.ElementTree")),
                                f"{ELEMENT_TREE} is gone: {_removed_in('3.9')}. Rewritten to xml.etree.ElementTree, "
                                "the same module since Python 3.3.")
        renames = {"base64": RENAMED_BASE64, "xml.etree": {"cElementTree": "ElementTree"}}.get(module, {})
        old = [n for n in (get_full_name_for_node(a.name) for a in original_node.names) if n in renames]
        if not old:
            return updated_node
        # Imported under the old name, so the references stay as they are.
        names = [a.with_changes(name=cst.Name(renames[n]), asname=a.asname or cst.AsName(name=cst.Name(n)))
                 if (n := get_full_name_for_node(a.name)) in renames else a for a in updated_node.names]
        return self.rewrite(original_node, updated_node.with_changes(names=names),
                            f"{module}.{', '.join(old)} is gone: {_removed_in('3.9')}. Imported as "
                            f"{', '.join(renames[n] for n in old)} instead, the same under its newer name.")

    def leave_Attribute(self, original_node: cst.Attribute, updated_node: cst.Attribute) -> cst.BaseExpression:
        qualified = self.qualified(original_node)
        for q in qualified:
            module, _, name = q.rpartition(".")
            if module == "collections" and name in ABCS:
                self.needs_abc_import = True
                new = updated_node.with_changes(value=cst.Attribute(value=updated_node.value, attr=cst.Name("abc")))
                return self.rewrite(original_node, new, f"collections.{name} is gone: {_removed_in('3.10')}. "
                                    f"Rewritten to collections.abc.{name}, the same class since Python 3.3.")
            if module == "base64" and name in RENAMED_BASE64:
                return self.rewrite(original_node, updated_node.with_changes(attr=cst.Name(RENAMED_BASE64[name])),
                                    f"base64.{name} is gone: {_removed_in('3.9')}. Rewritten to "
                                    f"base64.{RENAMED_BASE64[name]}, the same function under its newer name.")
            if q in REPORTED:
                self._report_removed(original_node, q)
        return updated_node

    def _report_removed(self, node: cst.CSTNode, name: str) -> None:
        version, instead = REPORTED[name]
        self.report(node, f"{name} is gone: {_removed_in(version)}. Use {instead}.", "high" if _always(version) else "medium")

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        if not self.needs_abc_import or self.has_abc_import:
            return updated_node
        body = list(updated_node.body)
        body.insert(_cst.after_leading_imports(body), cst.parse_statement("import collections.abc\n"))
        return updated_node.with_changes(body=body)
