"""Library APIs a Spark 2 job's Python used that the libraries a Spark 3.5.5
environment installs have removed.

A Spark 3.5.5 job runs on Python 3.8 to 3.11, where pip installs current
releases of anything left unpinned: numpy 1.24 or later, PyYAML 6, and pandas
2 unless the requirements cap it (spark-migrate-cli caps a pandas it raises
below 2). Code written against the old releases then fails at the call (the
2026-10-19 user trial: customer-360's tests failed on each of these):

  - Rewritten, because the replacement is the same thing on every release:
    numpy's aliases of the builtin types (`np.float`, `np.int`, `np.bool`,
    `np.object`, `np.str`, `np.complex`, `np.long`, `np.unicode`; removed in
    numpy 1.24) to the builtins; pandas' `.iteritems()` (removed in 2.0) to
    `.items()` and `.is_monotonic` to `.is_monotonic_increasing`; and
    `yaml.load(text)` without a Loader (a TypeError from PyYAML 6) to
    `yaml.load(text, Loader=yaml.FullLoader)`, what PyYAML 5.1 to 5.4 used
    when none was given.
  - Reported, because the replacement is written differently:
    `DataFrame.append` (use `pd.concat`) and `pd.np` (import numpy).

pandas' rules apply only in a module that imports pandas, where `.iteritems()`
and a used `.append(...)` result are pandas'.
"""
from __future__ import annotations

import libcst as cst
from libcst.helpers import get_full_name_for_node

from spark_upgrade_rules.detector import Fixer

NUMPY_ALIASES = {"float": "float", "int": "int", "bool": "bool", "object": "object", "str": "str",
                 "complex": "complex", "long": "int", "unicode": "str"}


class PythonLibraryRemoved(Fixer):
    """See the module docstring."""

    rule_id = "PythonLibraryRemoved"

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        self.uses_pandas = False
        # Calls whose result is thrown away: a list's append, never a DataFrame's.
        self.statements: set[int] = set()

    def visit_Module(self, node: cst.Module) -> None:
        class Imports(cst.CSTVisitor):
            found = False

            def visit_Import(self, n: cst.Import) -> None:
                if any(get_full_name_for_node(a.name).split(".")[0] == "pandas" or
                       get_full_name_for_node(a.name).startswith("pyspark.pandas") for a in n.names):
                    Imports.found = True

            def visit_ImportFrom(self, n: cst.ImportFrom) -> None:
                name = get_full_name_for_node(n.module) if n.module is not None else ""
                if name.split(".")[0] == "pandas" or name.startswith("pyspark.pandas"):
                    Imports.found = True

        node.visit(Imports())
        self.uses_pandas = Imports.found

    def visit_Expr(self, node: cst.Expr) -> None:
        self.statements.add(id(node.value))

    def leave_Attribute(self, original_node: cst.Attribute, updated_node: cst.Attribute) -> cst.BaseExpression:
        for q in self.qualified(original_node):
            module, _, name = q.rpartition(".")
            if module == "numpy" and name in NUMPY_ALIASES:
                return self.rewrite(original_node, cst.Name(NUMPY_ALIASES[name]),
                                    f"numpy.{name} is gone: numpy 1.24 removed the aliases of the builtin types, and pip "
                                    f"installs a later numpy on the Python Spark 3.5.5 runs on unless it is pinned. "
                                    f"Rewritten to the builtin {NUMPY_ALIASES[name]}, which it always was.")
            if q == "pandas.np":
                self.report(original_node, "pandas.np is gone: pandas 2.0 removed it (deprecated since 1.0). Import numpy "
                                           "and use it directly.", "high")
        if self.uses_pandas and original_node.attr.value == "is_monotonic":
            return self.rewrite(original_node, updated_node.with_changes(attr=cst.Name("is_monotonic_increasing")),
                                ".is_monotonic is gone: pandas 2.0 removed it. Rewritten to .is_monotonic_increasing, "
                                "which it always meant.")
        return updated_node

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.BaseExpression:
        func = original_node.func
        if isinstance(func, cst.Attribute):
            method = func.attr.value
            if self.uses_pandas and method == "iteritems" and not original_node.args:
                return self.rewrite(original_node, updated_node.with_changes(
                    func=updated_node.func.with_changes(attr=cst.Name("items"))),
                    ".iteritems() is gone: pandas 2.0 removed Series.iteritems and DataFrame.iteritems. Rewritten to "
                    ".items(), the same iterator.")
            if (self.uses_pandas and method == "append" and id(original_node) not in self.statements
                    and not isinstance(func.value, (cst.List, cst.ListComp))):
                self.report(original_node, "DataFrame.append is gone: pandas 2.0 removed it, so this fails with an "
                                           "AttributeError once pandas 2 is installed. Use "
                                           f"pd.concat([{self.code(func.value)}, ...]) instead (ignore_index and sort "
                                           "carry over as concat's arguments).", "medium")
        if "yaml.load" in self.qualified(func) and not any(a.keyword is not None and a.keyword.value == "Loader"
                                                           for a in original_node.args):
            positional = [a for a in original_node.args if a.keyword is None]
            if len(positional) == 1 and isinstance(func, cst.Attribute):
                loader = cst.Arg(keyword=cst.Name("Loader"), value=cst.Attribute(value=func.value, attr=cst.Name("FullLoader")),
                                 equal=cst.AssignEqual(whitespace_before=cst.SimpleWhitespace(""),
                                                       whitespace_after=cst.SimpleWhitespace("")))
                args = [*updated_node.args[:-1], updated_node.args[-1].with_changes(comma=cst.Comma(
                    whitespace_after=cst.SimpleWhitespace(" "))), loader]
                return self.rewrite(original_node, updated_node.with_changes(args=args),
                                    "yaml.load without a Loader fails on PyYAML 6 (a TypeError), which pip installs on "
                                    "the Python Spark 3.5.5 runs on unless it is pinned. Rewritten with "
                                    "Loader=FullLoader, what PyYAML 5.1 to 5.4 used when none was given.")
            if len(positional) == 1:
                self.report(original_node, "yaml.load without a Loader fails on PyYAML 6 (a TypeError). Pass "
                                           "Loader=yaml.FullLoader, what PyYAML 5.1 to 5.4 used when none was given, or use "
                                           "yaml.safe_load if the files hold plain data.", "high")
        return updated_node
