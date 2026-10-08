"""Detection-only rules: visitors that report findings and never edit the tree.

Every port of a Scala detector (PYSPARK.md SS11.8) is one of these. They return
structured findings -- rule id, line, and a message written for that call site
-- instead of writing a trailing `# <id>: ...` comment the way PySparkler's
StatementLineCommentWriter does. The comment channel was the plan (SS11.3) and
was measured to lose exactly what a detector exists to say: the JVM side reads
only *that a line changed*, so the per-site explanation ("pattern 'YYYY' uses
the week-based year") collapses to the rule's generic description, and a match
inside a multi-line statement lands on the statement's last line, not the
call. A rule that rewrites code is a transformer; a rule that reports is this.

A detector's id is its class attribute `rule_id`, a literal -- spark-migrate-cli's
PyRuleRegistryDriftSpec reads it from this source.
"""
from __future__ import annotations

import libcst as cst
from libcst.helpers import get_full_name_for_node
from libcst.metadata import MetadataWrapper, PositionProvider, QualifiedNameProvider

from spark_upgrade_rules import _cst
from spark_upgrade_rules.sql import sql_definitions


class Context:
    """Per-file facts every detector may need, computed once."""

    def __init__(self, module: cst.Module) -> None:
        self.module = module
        self.bindings = _cst.bindings_of(module)
        self.star_functions = _StarImports.of(module, _cst.FUNCTIONS)
        self.star_types = _StarImports.of(module, _cst.TYPES)
        # SQL statements written as constants, registry entries or templates
        # rather than as a call's literal (sql.py); a rule that reads SQL text
        # checks these too, at the line they are written.
        self.sql_definitions = sql_definitions(module)


class _StarImports(cst.CSTVisitor):
    def __init__(self, module_name: str) -> None:
        self.module_name = module_name
        self.found = False

    @classmethod
    def of(cls, module: cst.Module, module_name: str) -> bool:
        v = cls(module_name)
        module.visit(v)
        return v.found

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        if isinstance(node.names, cst.ImportStar) and node.module is not None:
            if get_full_name_for_node(node.module) == self.module_name:
                self.found = True


class Detector(cst.CSTVisitor):
    """Base class. Subclasses set `rule_id` and call `report`."""

    rule_id = ""
    METADATA_DEPENDENCIES = (PositionProvider, QualifiedNameProvider)

    def __init__(self, ctx: Context) -> None:
        super().__init__()
        self.ctx = ctx
        self.found: list[dict] = []

    def report(self, node: cst.CSTNode, message: str, confidence: str | None = None) -> None:
        finding = {"rule": self.rule_id, "line": self.get_metadata(PositionProvider, node).start.line, "message": message}
        if confidence:
            finding["confidence"] = confidence
        self.found.append(finding)

    def qualified(self, node: cst.CSTNode) -> set[str]:
        try:
            return {q.name for q in self.get_metadata(QualifiedNameProvider, node, set())}
        except Exception:  # pylint: disable=broad-except
            return set()

    def _resolves_to(self, func: cst.BaseExpression, module_name: str, star: bool, names: tuple[str, ...]) -> str | None:
        for q in self.qualified(func):
            prefix, _, short = q.rpartition(".")
            if prefix == module_name and short in names:
                return short
        # `from pyspark.sql.functions import *` leaves names unresolvable;
        # accept a bare name the module does not bind itself.
        if star and isinstance(func, cst.Name) and func.value in names and func.value not in self.ctx.bindings.count:
            return func.value
        return None

    def function(self, call: cst.Call, *names: str) -> str | None:
        """The `pyspark.sql.functions` function `call` invokes, if it is one of `names`."""
        return self._resolves_to(call.func, _cst.FUNCTIONS, self.ctx.star_functions, names)

    def type_name(self, call: cst.Call, *names: str) -> str | None:
        """The `pyspark.sql.types` class `call` constructs, if it is one of `names`."""
        return self._resolves_to(call.func, _cst.TYPES, self.ctx.star_types, names)

    def string(self, node: cst.BaseExpression | None) -> str | None:
        return _cst.string_or_bound(node, self.ctx.bindings)

    def sql_text_args(self, call: cst.Call) -> list[str]:
        """SQL text handed to Spark as a string: `spark.sql(...)`, `expr(...)`,
        `selectExpr(...)`, and a string `filter(...)`/`where(...)` condition --
        literal or bound once to a literal."""
        name = _cst.method_name(call)
        if name == "sql" or name in ("filter", "where"):
            text = self.string(_cst.argument(call, 0, "sqlQuery" if name == "sql" else "condition"))
            return [text] if text is not None else []
        if name == "selectExpr":
            return [t for t in (self.string(a) for a in _cst.positional(call)) if t is not None]
        if self.function(call, "expr"):
            text = self.string(_cst.argument(call, 0, "str"))
            return [text] if text is not None else []
        return []

    def check_sql(self, node: cst.CSTNode, sql: str) -> None:
        """Override to check SQL text; called for every call's SQL arguments and
        for every defined statement."""

    def visit_Call(self, node: cst.Call) -> None:
        for sql in self.sql_text_args(node):
            self.check_sql(node, sql)

    def leave_Module(self, original_node: cst.Module) -> None:
        for node, sql in self.ctx.sql_definitions:
            self.check_sql(node, sql)


def run(module: cst.Module, detectors: list[type], run_ids: set) -> tuple[list[dict], list[dict]]:
    """Every finding from the detectors in `run_ids`, in source order, and one
    error record per detector that raised -- a detector failing never hides
    another's findings."""
    chosen = [d for d in detectors if d.rule_id in run_ids]
    if not chosen:
        return [], []
    ctx = Context(module)
    wrapper = MetadataWrapper(module, unsafe_skip_copy=True)
    found, errors = [], []
    for cls in chosen:
        detector = cls(ctx)
        try:
            wrapper.visit(detector)
        except Exception as e:  # pylint: disable=broad-except
            errors.append({"rule": cls.rule_id, "error": str(e)})
            continue
        found.extend(detector.found)
    found.sort(key=lambda f: (f["line"], f["rule"]))
    return found, errors
