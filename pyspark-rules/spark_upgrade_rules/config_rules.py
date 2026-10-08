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
