"""Our own PySpark migration rules, run beside the vendored PySparkler.

The contract with spark-migrate-cli's bridge script:

  transformers()           PySparkler-style transformers, merged with its own (none)
  detector_ids()           ids of every rule below
  detect(module, run_ids)  their findings for one parsed file -- a Fixer's
                           rewritable sites as Tier 1, with the rewrite shown
  fixer_ids()              the rules that can rewrite (detector.Fixer)
  fix(module, fix_ids)     codegen: those rewrites applied, chained, to one file
  extract_sql(module)      every `.sql(...)` call site, for the JVM-side SQL lint

The bridge runs only the ids spark-migrate-cli's PyRuleRegistry names. See
README.md for the conventions.
"""
from __future__ import annotations

import libcst as cst

from spark_upgrade_rules import detector as _detector
from spark_upgrade_rules.config_rules import CommandOutputSchemaDetect, RemovedSqlConfigDetect, SetCommandSparkConfDetect
from spark_upgrade_rules.dataframe_rules import NaFunctionsNameMatchDetect, SelfJoinAmbiguousColumnDetect
from spark_upgrade_rules.datasource_rules import (
    CsvBinaryTypeDetect,
    CsvBomMultilineDetect,
    JsonEmptyStringDetect,
    MultiLineDatasetReadWarn,
    PathOptionConflictDetect,
    SaveAsTablePathDetect,
    StoreAssignmentPolicyDetect,
)
from spark_upgrade_rules.datetime_rules import (
    AddMonthsSnapDetect,
    DateTimeStringComparisonDetect,
    DateTimeFormatPatternValidator,
    DateTimeStrictParsingDetect,
    InvalidTimeZoneIdDetect,
)
from spark_upgrade_rules.function_rules import (
    DuplicateMapKeyLiteralDetect,
    EmptyCollectionTypeDetect,
    GroupingIdTypeDetect,
    HashOnMapTypeDetect,
    MapTypeKeyInCreateMapDetect,
    NegativeDecimalScaleDetect,
    SplitEmptyRegexDetect,
)
from spark_upgrade_rules.koalas_rules import KoalasToPandasOnSpark, PandasOnSparkDropAxis, PandasOnSparkSqlFormatter
from spark_upgrade_rules.pandas_on_spark_rules import (
    PandasOnSparkAstypeCategory,
    PandasOnSparkConcatSort,
    PandasOnSparkGroupByApplyInference,
    PandasOnSparkGroupByHeadTailNegative,
    PandasOnSparkIndexInsertBounds,
    PandasOnSparkSeriesModeName,
)
from spark_upgrade_rules.pyspark_api_rules import MlSharedParamSetters, RemovedPySparkApi, RowKwargsFieldOrder
from spark_upgrade_rules.pyspark_guide_rules import ArrayTypeSchemaInference, NamedtupleCloudpickle
from spark_upgrade_rules.python_library_rules import PythonLibraryRemoved
from spark_upgrade_rules.python_stdlib_rules import PythonStdlibRemoved
from spark_upgrade_rules.sql import extract_sql
from spark_upgrade_rules.star_import_rules import StarImportShadowsFilter

__all__ = ["detect", "detector_ids", "extract_sql", "fix", "fixer_ids", "transformers"]

# Ports of the Scala detectors (PYSPARK.md SS11.8), by family, then the PySpark-only rules.
DETECTORS = [
    # D -- datetime
    DateTimeFormatPatternValidator,
    DateTimeStrictParsingDetect,
    AddMonthsSnapDetect,
    InvalidTimeZoneIdDetect,
    DateTimeStringComparisonDetect,
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
    RemovedSqlConfigDetect,
    CommandOutputSchemaDetect,
    # H -- data sources
    PathOptionConflictDetect,
    JsonEmptyStringDetect,
    CsvBinaryTypeDetect,
    CsvBomMultilineDetect,
    MultiLineDatasetReadWarn,
    StoreAssignmentPolicyDetect,
    SaveAsTablePathDetect,
    # PySpark API: removals between 2.4.8 and 3.5.5, and the Row and ML-setter
    # rewrites. MlSharedParamSetters adds lines, so it rewrites last.
    RowKwargsFieldOrder,
    RemovedPySparkApi,
    # 3.5.5's star import of pyspark.sql.functions binds names 2.4.8's did not.
    StarImportShadowsFilter,
    # Standard-library names the job's old Python had and 3.8-3.11 do not; after
    # the star-import rule, so an import it repeats is rewritten too.
    PythonStdlibRemoved,
    # numpy, pandas and PyYAML APIs the releases pip installs on Python 3.8-3.11 removed.
    PythonLibraryRemoved,
    # Koalas -> pandas API on Spark, then the two changes the port reaches; the
    # port runs first, so codegen's later rules see pyspark.pandas.
    KoalasToPandasOnSpark,
    PandasOnSparkDropAxis,
    PandasOnSparkSqlFormatter,
    MlSharedParamSetters,
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


def fixer_ids() -> list[str]:
    return [d.rule_id for d in DETECTORS if issubclass(d, _detector.Fixer)]


def fix(module: cst.Module, fix_ids: set) -> tuple[str, list[dict], list[dict]]:
    """The file's code with every rewrite in `fix_ids` applied, the sites
    rewritten, and `{"rule", "error"}` for any rule that raised."""
    rewritten, applied, errors = _detector.apply(module, DETECTORS, fix_ids)
    return rewritten.code, applied, errors


def detect(module: cst.Module, run_ids: set) -> tuple[list[dict], list[dict]]:
    """Findings `{"rule", "line", "message"[, "confidence"]}` from every
    detector in `run_ids`, in source order, and `{"rule", "error"}` for any
    detector that raised."""
    return _detector.run(module, DETECTORS, run_ids)
