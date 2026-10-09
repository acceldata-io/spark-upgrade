"""Family H: DataFrameReader / DataFrameWriter behaviour changes. Ports of the
Scala rules of the same ids, matched on the reader chain's shape -- option
calls, `options(...)`, and the keyword options PySpark's `csv()`/`json()`
take directly (`spark.read.csv(p, multiLine=True)`), which have no Scala
counterpart and are how most PySpark code sets them.
"""
from __future__ import annotations

import re

import libcst as cst

from spark_upgrade_rules import _cst
from spark_upgrade_rules.detector import Detector, Fixer

READERS = {"read", "readStream"}
WRITERS = {"write", "writeStream"}
# Terminal calls that take a path, and the name of that parameter.
PATH_CALLS = {"load": "path", "save": "path", "start": "path", "csv": "path", "json": "path",
              "parquet": "paths", "orc": "path", "text": "paths"}


def _is_read(node: cst.Call) -> bool:
    return bool(_cst.spine_attrs(node) & READERS)


class _ReaderHelpers:
    def options(self, node: cst.Call) -> dict[str, cst.BaseExpression]:
        return _cst.options_set(node, self.ctx.bindings)

    def schema_of(self, node: cst.Call) -> cst.BaseExpression | None:
        """The explicit schema of a read: `.schema(s)` on the chain, or `schema=`."""
        own = _cst.keyword(node, "schema")
        if own is not None:
            return own
        calls = _cst.spine_calls(_cst.receiver(node), "schema")
        return _cst.argument(calls[0], 0, "schema") if calls else None

    def resolved(self, node: cst.BaseExpression | None) -> cst.BaseExpression | None:
        """A name bound once in the file is followed one hop, to what it was bound to."""
        if isinstance(node, cst.Name):
            return self.ctx.bindings.expression(node.value) or node
        return node


class _ReaderDetector(_ReaderHelpers, Detector):
    pass


class PathOptionConflictDetect(_ReaderHelpers, Fixer):
    """A 'path' option and a path argument on one read or write. Spark 3.1
    rejects the pair; 3.0 and below let a single path argument overwrite the
    option -- so dropping the option is exactly 2.4, and is the rewrite. Not
    for a read given several paths (2.4 added the option to them), nor where
    the option is not a literal `.option`/`.options` keyword: those keep
    Tier 2, spark.sql.legacy.pathOptionBehavior.enabled."""

    rule_id = "PathOptionConflictDetect"

    def _single_path(self, node: cst.BaseExpression | None) -> bool:
        """An expression that is certainly one string, not a list of paths."""
        if isinstance(node, cst.Name):
            node = self.ctx.bindings.expression(node.value) or node
        if isinstance(node, (cst.SimpleString, cst.ConcatenatedString, cst.FormattedString)):
            return True
        if isinstance(node, cst.BinaryOperation) and isinstance(node.operator, (cst.Add, cst.Modulo)):
            return self._single_path(node.left) or self._single_path(node.right)
        if isinstance(node, cst.Call):
            if _cst.method_name(node) == "format" and self._single_path(_cst.receiver(node)):
                return True
            return any(q in ("os.path.join", "posixpath.join", "builtins.str") for q in self.qualified(node.func))
        return False

    def _without_path_option(self, node: cst.BaseExpression) -> cst.BaseExpression | None:
        """`node`'s spine with the literal path option removed, or None."""
        if not isinstance(node, cst.Call) or not isinstance(node.func, cst.Attribute):
            return None
        name = _cst.method_name(node)
        if name == "option" and (self.string(_cst.argument(node, 0, "key")) or "").lower() == "path":
            return node.func.value
        if name == "options" and any(a.keyword is not None and a.keyword.value.lower() == "path" for a in node.args):
            kept = [a for a in node.args if not (a.keyword is not None and a.keyword.value.lower() == "path")]
            if not kept:
                return node.func.value
            kept[-1] = kept[-1].with_changes(comma=cst.MaybeSentinel.DEFAULT)
            return node.with_changes(args=kept)
        inner = self._without_path_option(node.func.value)
        return None if inner is None else node.with_changes(func=node.func.with_changes(value=inner))

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.BaseExpression:
        name = _cst.method_name(original_node)
        param = PATH_CALLS.get(name or "")
        attrs = _cst.spine_attrs(original_node)
        if param is None or not (attrs & (READERS | WRITERS)):
            return updated_node
        paths = _cst.positional(original_node)
        keyword = _cst.keyword(original_node, param) or _cst.keyword(original_node, "path")
        if not (paths or keyword is not None):
            return updated_node
        options = _cst.options_set(original_node, self.ctx.bindings, own_keywords=False)
        if not ({"path", "paths"} & options.keys()):
            return updated_node
        message = (f"A 'path' option is set on this chain and .{name}(...) is also given a path. Spark 3.1 rejects the "
                   "pair (\"There is a 'path' or 'paths' option set and load() is called with path parameters\"); 3.0 "
                   "and below let the path argument overwrite the option.")
        given = keyword if keyword is not None else paths[0]
        single = bool(attrs & WRITERS) or (len(paths) + (keyword is not None) == 1 and self._single_path(given))
        receiver = self._without_path_option(updated_node.func.value) if single and "paths" not in options else None
        if receiver is None:
            self.report(original_node, message + " Drop one, or set spark.sql.legacy.pathOptionBehavior.enabled=true.")
            return updated_node
        return self.rewrite(original_node, updated_node.with_changes(func=updated_node.func.with_changes(value=receiver)),
                            message + " Rewritten without the option, which is what 2.4 read or wrote.")


class JsonEmptyStringDetect(_ReaderDetector):
    """A JSON read with an explicit schema reads `""` in a non-string field as a
    malformed record from 3.0, where 2.4 read it as null. Probed on 3.5.5: in the
    default PERMISSIVE mode the field still comes back null, so the visible
    difference needs FAILFAST (the job fails), DROPMALFORMED (the row is
    dropped) or a corrupt-record column (it fills). The Scala rule flags every
    JSON read with a schema; this one only those whose mode makes it visible,
    or whose mode it cannot read."""

    rule_id = "JsonEmptyStringDetect"

    def visit_Call(self, node: cst.Call) -> None:
        if _cst.method_name(node) != "json" or not _is_read(node) or self.schema_of(node) is None:
            return
        options = self.options(node)
        mode_node = options.get("mode")
        mode = self.string(mode_node)
        corrupt = "columnnameofcorruptrecord" in options or "_corrupt_record" in " ".join(
            _cst.iter_strings(self.resolved(self.schema_of(node))))
        if mode is not None and mode.upper() in ("FAILFAST", "DROPMALFORMED"):
            effect, confidence = ("fails the job" if mode.upper() == "FAILFAST" else "drops the whole row"), "high"
        elif corrupt:
            effect, confidence = "fills the corrupt-record column", "medium"
        elif mode_node is not None and mode is None:
            effect, confidence = "fails or drops the row under FAILFAST/DROPMALFORMED (the mode here is not a literal)", "low"
        else:
            return
        self.report(node, f"JSON read with an explicit schema: from Spark 3.0 an empty string in a non-string field (int, "
                          f"bigint, boolean, decimal) makes the record malformed, which here {effect}; 2.4 read it as null. "
                          "Clean the data or widen the field to string, or set "
                          "spark.sql.legacy.json.allowEmptyString.enabled=true.", confidence)


class CsvBinaryTypeDetect(_ReaderDetector):
    rule_id = "CsvBinaryTypeDetect"

    def _declares_binary(self, schema: cst.BaseExpression | None) -> bool:
        schema = self.resolved(schema)
        if schema is None:
            return False
        ddl = _cst.string_value(schema)
        if ddl is not None:
            return re.search(r"(?i)(?:^|[\s:,<(])binary\b", ddl) is not None
        stack = [schema]
        while stack:
            n = stack.pop()
            if isinstance(n, cst.Call) and self.type_name(n, "BinaryType"):
                return True
            if isinstance(n, cst.Name) and n is not schema:
                bound = self.ctx.bindings.expression(n.value)
                if bound is not None and isinstance(bound, cst.Call) and self.type_name(bound, "StructType", "StructField"):
                    stack.append(bound)
            stack.extend(n.children)
        return False

    def visit_Call(self, node: cst.Call) -> None:
        if _cst.method_name(node) == "csv" and _is_read(node) and self._declares_binary(self.schema_of(node)):
            self.report(node, "This CSV read declares a binary column. Spark 3.4 removed BinaryType from the CSV data source "
                              "(\"The CSV datasource doesn't support the column ... of the type BINARY\"), for reads and writes; "
                              "no config restores it. Base64-encode the column as a string, or use Parquet.")


class CsvBomMultilineDetect(_ReaderDetector):
    rule_id = "CsvBomMultilineDetect"

    def visit_Call(self, node: cst.Call) -> None:
        if _cst.method_name(node) != "csv" or not _is_read(node):
            return
        options = self.options(node)
        if _cst.is_true(options.get("multiline")) and not ({"encoding", "charset"} & options.keys()):
            self.report(node, "CSV read with multiLine=true and no encoding option. Spark 3.0 stopped detecting the encoding "
                              "from a byte-order mark in multi-line mode and reads every file as UTF-8, so a UTF-16 or UTF-32 "
                              "file is read as garbage (a UTF-8 BOM is still skipped -- probed on 3.5.5). Set the encoding the "
                              "files actually use (.option('encoding', 'UTF-16')). No config restores it.")


class MultiLineDatasetReadWarn(_ReaderDetector):
    rule_id = "MultiLineDatasetReadWarn"

    def visit_Call(self, node: cst.Call) -> None:
        if _cst.method_name(node) not in ("csv", "json", "load") or not _is_read(node):
            return
        if _cst.is_true(self.options(node).get("multiline")) and "linesep" not in self.options(node):
            self.report(node, "Multi-line text read with no lineSep. In 2.4, reading multi-line input with \\r\\n (Windows) "
                              "line ends could leave \\r characters in values; 3.x does not. Set lineSep='\\n' to keep the old "
                              "behaviour -- though for most jobs it was the bug. This rule is fuzzy.", "low")
