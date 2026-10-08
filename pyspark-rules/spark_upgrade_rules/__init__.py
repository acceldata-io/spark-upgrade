"""Our own PySpark migration rules, run beside the vendored PySparkler.

The contract with spark-migrate-cli's bridge script:

  transformers()           rules that rewrite code, merged with PySparkler's own (none yet)
  detector_ids()           ids of the detection-only rules below
  detect(module, run_ids)  their findings for one parsed file
  extract_sql(module)      every `.sql(...)` call site, for the JVM-side SQL lint

The bridge runs only the ids spark-migrate-cli's PyRuleRegistry names. See
README.md for the conventions.
"""
from __future__ import annotations

import libcst as cst

from spark_upgrade_rules import detector as _detector
from spark_upgrade_rules.config_rules import SetCommandSparkConfDetect
from spark_upgrade_rules.dataframe_rules import NaFunctionsNameMatchDetect, SelfJoinAmbiguousColumnDetect
from spark_upgrade_rules.datasource_rules import (
    CsvBinaryTypeDetect,
    CsvBomMultilineDetect,
    JsonEmptyStringDetect,
    MultiLineDatasetReadWarn,
    PathOptionConflictDetect,
)
from spark_upgrade_rules.datetime_rules import AddMonthsSnapDetect, DateTimeFormatPatternValidator, InvalidTimeZoneIdDetect
from spark_upgrade_rules.function_rules import (
    DuplicateMapKeyLiteralDetect,
    EmptyCollectionTypeDetect,
    GroupingIdTypeDetect,
    HashOnMapTypeDetect,
    MapTypeKeyInCreateMapDetect,
    NegativeDecimalScaleDetect,
    SplitEmptyRegexDetect,
)
from spark_upgrade_rules.pandas_on_spark_rules import (
    PandasOnSparkAstypeCategory,
    PandasOnSparkConcatSort,
    PandasOnSparkGroupByApplyInference,
    PandasOnSparkGroupByHeadTailNegative,
    PandasOnSparkIndexInsertBounds,
    PandasOnSparkSeriesModeName,
)
from spark_upgrade_rules.pyspark_guide_rules import ArrayTypeSchemaInference, NamedtupleCloudpickle
from spark_upgrade_rules.sql import extract_sql

__all__ = ["detect", "detector_ids", "extract_sql", "transformers"]

# Ports of the Scala detectors (PYSPARK.md SS11.8), by family, then the PySpark-only rules.
DETECTORS = [
    # D -- datetime
    DateTimeFormatPatternValidator,
    AddMonthsSnapDetect,
    InvalidTimeZoneIdDetect,
    # E, F -- types and built-in functions
    NegativeDecimalScaleDetect,
    DuplicateMapKeyLiteralDetect,
    MapTypeKeyInCreateMapDetect,
    EmptyCollectionTypeDetect,
    HashOnMapTypeDetect,
    SplitEmptyRegexDetect,
    # G -- DataFrame API and session config
    GroupingIdTypeDetect,
    SelfJoinAmbiguousColumnDetect,
    NaFunctionsNameMatchDetect,
    SetCommandSparkConfDetect,
    # H -- data sources
    PathOptionConflictDetect,
    JsonEmptyStringDetect,
    CsvBinaryTypeDetect,
    CsvBomMultilineDetect,
    MultiLineDatasetReadWarn,
    # The PySpark guide's own 3.3 -> 3.4 changes (PYSPARK.md G14): core...
    ArrayTypeSchemaInference,
    NamedtupleCloudpickle,
    # ...and pandas API on Spark, run only in a repo that uses it.
    PandasOnSparkGroupByHeadTailNegative,
    PandasOnSparkGroupByApplyInference,
    PandasOnSparkIndexInsertBounds,
    PandasOnSparkSeriesModeName,
    PandasOnSparkAstypeCategory,
    PandasOnSparkConcatSort,
]


def transformers() -> list:
    """Every rewriting rule in this package, freshly constructed -- transformers
    are visitors and may keep state between visits, so callers build a new list
    per file."""
    return []


def detector_ids() -> list[str]:
    return [d.rule_id for d in DETECTORS]


def detect(module: cst.Module, run_ids: set) -> tuple[list[dict], list[dict]]:
    """Findings `{"rule", "line", "message"[, "confidence"]}` from every
    detector in `run_ids`, in source order, and `{"rule", "error"}` for any
    detector that raised."""
    return _detector.run(module, DETECTORS, run_ids)
