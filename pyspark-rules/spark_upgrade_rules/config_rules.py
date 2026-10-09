"""`spark.conf.set` on a core Spark config -- the DataFrame-API shape of the
SQL `SET` change the Scala SetCommandSparkConfDetect reads in SQL text.

Spark 3.0 made RuntimeConfig reject a core (SparkConf) key: both `SET k=v` and
`spark.conf.set(k, v)` go through `RuntimeConfig.requireNonStaticConf`, which
throws CANNOT_MODIFY_CONFIG when `ConfigEntry.findEntry(k) != null` and `k` is
not a SQL config. In 2.4 the call was accepted and did nothing. The keys are
Spark 3.5.5's own (data/spark_core_config_keys_3_5_5.txt), so a Hive key
(`hive.exec.dynamic.partition.mode`), an app key (`spark.myapp.x`) or a
`spark.hadoop.*` key -- all of which 3.5.5 accepts -- never fire.
"""
from __future__ import annotations

import re
from importlib import resources

import libcst as cst

from spark_upgrade_rules import _cst
from spark_upgrade_rules.detector import Detector


def _core_keys() -> frozenset[str]:
    text = resources.files("spark_upgrade_rules").joinpath("data/spark_core_config_keys_3_5_5.txt").read_text("utf-8")
    return frozenset(line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#"))


CORE_KEYS = _core_keys()


class SetCommandSparkConfDetect(Detector):
    rule_id = "SetCommandSparkConfDetect"

    def visit_Call(self, node: cst.Call) -> None:
        # `<session>.conf.set(...)`. Not `self.conf.set` or `conf.set`, which
        # are a SparkConf far more often than a session's RuntimeConfig.
        func = node.func
        if not (isinstance(func, cst.Attribute) and func.attr.value == "set" and isinstance(func.value, cst.Attribute)
                and func.value.attr.value == "conf"):
            return
        owner = func.value.value
        if isinstance(owner, cst.Name) and owner.value in ("self", "cls"):
            return
        key = self.string(_cst.argument(node, 0, "key"))
        if key in CORE_KEYS:
            self.report(node, f"spark.conf.set(\"{key}\", ...) sets a core Spark config at run time. Spark 3.0 rejects it "
                              "(CANNOT_MODIFY_CONFIG); 2.4 accepted it and ignored it. Set it at submit time (--conf, "
                              "spark-defaults.conf, or the session builder before the context starts), or set "
                              "spark.sql.legacy.setCommandRejectsSparkCoreConfs=false.", "high")


# SQL configs 2.4 read that 3.5.5 does not. Read off the real 3.5.5 SQLConf
# against 2.4.8's SQLConf.scala (spark-migrate-cli PYSPARK.md SS8.14): the
# first group is in SQLConf.removedSQLConfigs, which makes Spark throw when the
# key is set to anything but the default given here -- and through the session
# builder, the session cannot be created at all (probed). The second group is
# simply unknown to 3.5.5, so setting it does nothing and the 2.4 behaviour it
# switched is silently gone.
REMOVED = {
    "spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation": ("false", "3.x always refuses to create a managed "
        "table over a non-empty location: clear the directory first, or write an external table with a path option"),
    "spark.sql.execution.pandas.respectSessionTimeZone": ("true", "pandas conversions always use the session time zone"),
    "spark.sql.fromJsonForceNullableSchema": ("true", "from_json's schema is always made nullable"),
    "spark.sql.legacy.compareDateTimestampInTimestamp": ("true", "a date and a timestamp are always compared as timestamps"),
    "spark.sql.parquet.int64AsTimestampMillis": ("false", "set spark.sql.parquet.outputTimestampType=TIMESTAMP_MILLIS instead"),
}
IGNORED = {
    "spark.sql.adaptive.minNumPostShufflePartitions": "its successor is spark.sql.adaptive.coalescePartitions.minPartitionSize",
    "spark.sql.legacy.rdd.applyConf": "SQL configs are always applied to the RDDs a Dataset produces",
    "spark.sql.legacy.sources.write.passPartitionByAsOptions": "partitionBy columns are always passed to the source",
    "spark.sql.orc.copyBatchToSpark": "the ORC reader no longer has this copy path",
    "spark.sql.variable.substitute.depth": "variable substitution no longer has a depth setting",
}


class RemovedSqlConfigDetect(Detector):
    rule_id = "RemovedSqlConfigDetect"

    def visit_Call(self, node: cst.Call) -> None:
        if _cst.method_name(node) in ("config", "set", "setConf"):
            key = self.string(_cst.argument(node, 0, "key"))
            value = _cst.argument(node, 1, "value")
            if key is not None and value is not None:
                text = self.string(value)
                if text is None and isinstance(value, cst.Name) and value.value in ("True", "False"):
                    text = value.value.lower()
                self._check(node, key, text, value is not None and text is None)
        super().visit_Call(node)

    def check_sql(self, node: cst.CSTNode, sql: str) -> None:
        for m in re.finditer(r"(?im)^\s*SET\s+([\w.]+)\s*=\s*([^\s;]+)", sql):
            self._check(node, m.group(1), m.group(2).strip("'\""), False)

    def _check(self, node: cst.CSTNode, key: str, value: str | None, unknown: bool) -> None:
        if key in REMOVED:
            default, instead = REMOVED[key]
            if value is not None and value.strip().lower() == default:
                return
            self.report(node, f"{key} was removed in Spark 3.0. Setting it to anything but its default ({default}) throws "
                              "AnalysisException (\"The SQL config ... was removed\"); set on the session builder, the "
                              f"session cannot be created. The 2.4 behaviour it switched is gone -- {instead}.",
                        "medium" if unknown else "high")
        elif key in IGNORED:
            self.report(node, f"{key} does not exist in Spark 3.5.5: setting it does nothing, so the 2.4 behaviour it "
                              f"selected is silently lost -- {IGNORED[key]}.", "medium")


# Commands whose output columns 3.0 renamed, and the 2.4 names
# (spark.sql.legacy.keepCommandOutputSchema restores them -- probed on 3.5.5).
_COMMANDS = re.compile(r"(?is)^\s*(SHOW\s+(?:DATABASES|SCHEMAS)|SHOW\s+TABLES?\s+EXTENDED|SHOW\s+TABLES|"
                       r"DESC(?:RIBE)?\s+(?:DATABASE|SCHEMA)(?:\s+EXTENDED)?)\b")
_RENAMED = {
    "SHOW DATABASES": "databaseName -> namespace",
    "SHOW TABLES": "database -> namespace",
    "DESCRIBE DATABASE": "database_description_item/database_description_value -> info_name/info_value",
}
_OLD_COLUMNS = {"databaseName", "database", "database_description_item", "database_description_value"}


class CommandOutputSchemaDetect(Detector):
    """`SHOW DATABASES`, `SHOW TABLES` and `DESCRIBE DATABASE` name their output
    columns differently from 3.0 (probed on 3.5.5): code reading
    `row.databaseName` or `row["database"]` from the result fails. The legacy
    config restores 2.4's names for these commands only, so it is safe to set
    for a job written against them; high confidence where the file reads one
    of the old names."""

    rule_id = "CommandOutputSchemaDetect"

    def __init__(self, ctx) -> None:
        super().__init__(ctx)
        self.reads_old = any(
            (isinstance(n, cst.Attribute) and n.attr.value in _OLD_COLUMNS) or _cst.string_value(n) in _OLD_COLUMNS
            for n in _walk(ctx.module))

    def check_sql(self, node: cst.CSTNode, sql: str) -> None:
        m = _COMMANDS.match(sql)
        if not m:
            return
        command = " ".join(m.group(1).upper().split())
        kind = "SHOW DATABASES" if command.startswith(("SHOW DATABASES", "SHOW SCHEMAS")) else \
            "DESCRIBE DATABASE" if command.startswith("DESC") else "SHOW TABLES"
        self.report(node, f"{command}: Spark 3.0 renamed its output columns ({_RENAMED[kind]}). Code that reads the result "
                          "by the 2.4 name fails; positional access is unaffected. "
                          "spark.sql.legacy.keepCommandOutputSchema=true restores 2.4's names.",
                    "high" if self.reads_old else "low")


def _walk(node: cst.CSTNode):
    stack = [node]
    while stack:
        n = stack.pop()
        yield n
        stack.extend(n.children)
