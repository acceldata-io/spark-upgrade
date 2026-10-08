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
from spark_upgrade_rules.detector import Detector

READERS = {"read", "readStream"}
WRITERS = {"write", "writeStream"}
# Terminal calls that take a path, and the name of that parameter.
PATH_CALLS = {"load": "path", "save": "path", "start": "path", "csv": "path", "json": "path",
              "parquet": "paths", "orc": "path", "text": "paths"}


def _is_read(node: cst.Call) -> bool:
    return bool(_cst.spine_attrs(node) & READERS)


class _ReaderDetector(Detector):
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


class PathOptionConflictDetect(_ReaderDetector):
    rule_id = "PathOptionConflictDetect"

    def visit_Call(self, node: cst.Call) -> None:
        name = _cst.method_name(node)
        param = PATH_CALLS.get(name or "")
        if param is None or not (_cst.spine_attrs(node) & (READERS | WRITERS)):
            return
        has_path_arg = bool(_cst.positional(node)) or _cst.keyword(node, param) is not None
        options = _cst.options_set(node, self.ctx.bindings, own_keywords=False)
        if has_path_arg and ({"path", "paths"} & options.keys()):
            self.report(node, f"A 'path' option is set on this chain and .{name}(...) is also given a path. Spark 3.1 rejects "
                              "the pair (\"There is a 'path' or 'paths' option set and load() is called with path parameters\"); "
                              "2.4 let the argument win. Drop one, or set spark.sql.legacy.pathOptionBehavior.enabled=true.")


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
        if "multiline" in self.options(node) and "linesep" not in self.options(node):
            self.report(node, "Multi-line text read with no lineSep. In 2.4, reading multi-line input with \\r\\n (Windows) "
                              "line ends could leave \\r characters in values; 3.x does not. Set lineSep='\\n' to keep the old "
                              "behaviour -- though for most jobs it was the bug. This rule is fuzzy.", "low")
