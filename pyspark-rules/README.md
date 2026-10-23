# spark-upgrade-rules

PySpark migration rules for Spark 2.4 → 3.5. They run beside the vendored `../pysparkler`
engine, which is never edited, so that a pin bump never conflicts with these rules. A caller loads
both sets and passes the rule ids it wants to run.

This mirrors the other two languages: `../scalafix` holds the Scala rules, `../openrewrite` the
Java recipes, and this package the Python rules. A rule here carries its id and its per-site
message; how a caller ranks or documents a rule is up to the caller.

## What is in it

| Module | What |
|---|---|
| `spark_upgrade_rules/__init__.py` | The contract with a caller: `detector_ids()` and `detect(module, run_ids)` (every rule below, for analysis), `fixer_ids()` and `fix(module, fix_ids)` (the rules that rewrite, for codegen), `extract_sql(module)`; `transformers()` is PySparkler-shaped and empty |
| `detector.py` | `Detector`, the base of a rule that reports: a LibCST visitor that calls `report(node, message, confidence)` (`rule_id=` reports a site under another id the rule owns). `Fixer`, the base of a rule that also rewrites: a LibCST transformer whose `rewrite(original, new, message)` reports the site as Tier 1, with the change shown, and returns the new node. `run` (analysis: findings only) and `apply` (codegen: the rewrites, chained) |
| `pyspark_api_rules.py` | `RowKwargsFieldOrder`, `MlSharedParamSetters`, `RemovedPySparkApi` — the 2.4.8 → 3.5.5 PySpark API breaks, from a diff of 2.4.8's public API (names, class attributes, parameter lists) against a 3.5.5 import; the first two supersede PySparkler's `PY24-30-007`/`-008` with a rewrite |
| `star_import_rules.py` | `StarImportShadowsFilter` — 3.5.5's `from pyspark.sql.functions import *` binds 255 names 2.4.8's did not (no `__all__`): a reference to the builtin `filter` becomes `builtins.filter`, a name imported before the star import (`from functools import reduce`) is imported again after it, and a name defined before it is reported |
| `python_stdlib_rules.py` | `PythonStdlibRemoved` — standard-library names Python 3.6/3.7 had and the 3.8–3.11 Spark 3.5.5 runs on does not: the `collections` ABC aliases, `base64.encodestring`/`decodestring` and `xml.etree.cElementTree` are rewritten to their exact replacements, `json.loads`/`json.load`'s ignored `encoding` argument is dropped, and `Thread.isAlive()` becomes `is_alive()` in a file that imports `threading`; `time.clock`, `cgi.escape`, `platform.linux_distribution`/`dist`, `os.errno`, `fractions.gcd`, `inspect.getargspec` and an `isAlive()` elsewhere are reported |
| `datetime_rules.py` | `DateTimeFormatPatternValidator`, `AddMonthsSnapDetect` (a `Fixer`: restores 2.4's month-end snap where its arguments can be repeated and the generated column name is discarded — `.alias`/`.name`, a `withColumn` value, a `filter`/`where`, `orderBy`/`sort` or `join` condition — and reports the other sites, and `add_months` in SQL text, as `AddMonthsSnapDetect.Manual`), `InvalidTimeZoneIdDetect` — Family D; `DateTimeStrictParsingDetect`, the strict 3.x parser on a pattern that did not change; `DateTimeStringComparisonDetect`, a Column compared with a date-shaped string, only where a probe showed the answer changes |
| `function_rules.py` | `NegativeDecimalScaleDetect`, `DuplicateMapKeyLiteralDetect`, `MapTypeKeyInCreateMapDetect`, `EmptyCollectionTypeDetect`, `HashOnMapTypeDetect`, `SplitEmptyRegexDetect`, `GroupingIdTypeDetect` — Families E, F |
| `dataframe_rules.py` | `SelfJoinAmbiguousColumnDetect` (and, under `.Manual`, a join in a loop of frames the loop relinks, which 3.5 fails with `MISSING_ATTRIBUTES` whatever the config), `NaFunctionsNameMatchDetect` — Family G |
| `config_rules.py` | `SetCommandSparkConfDetect` — `spark.conf.set` on a core config, against `data/spark_core_config_keys_3_5_5.txt`; `RemovedSqlConfigDetect` — a 2.4 SQL config 3.5.5 removed or ignores; `CommandOutputSchemaDetect` — `SHOW DATABASES`/`TABLES`, `DESCRIBE DATABASE`'s renamed columns |
| `datasource_rules.py` | `PathOptionConflictDetect`, `JsonEmptyStringDetect`, `CsvBinaryTypeDetect`, `CsvBomMultilineDetect`, `MultiLineDatasetReadWarn` — Family H; `PathOptionConflictDetect` rewrites only `load(path)` with one path and the writers, and leaves the `csv`/`json`/`parquet`/`orc`/`text` readers on the config (2.4 read the option's path and the argument there). `StoreAssignmentPolicyDetect` — `insertInto`, or `saveAsTable` in append mode, under 3.0's ANSI store assignment. `SaveAsTablePathDetect` — `saveAsTable` with a 'path' option in the default mode, which 3.2 fails on a non-empty path |
| `pyspark_guide_rules.py` | `ArrayTypeSchemaInference`, `NamedtupleCloudpickle` — the PySpark guide's own 3.3 → 3.4 core changes, no Scala rule to port |
| `koalas_rules.py` | `KoalasToPandasOnSpark`, `PandasOnSparkDropAxis`, `PandasOnSparkSqlFormatter` — the Koalas → `pyspark.pandas` port and the two 3.2 → 3.3 changes it reaches, all rewriting where the receiver is provably a Koalas or pandas-on-Spark frame (`args.koalas` and a Series's `drop(axis=1)` are reported, not rewritten); from a diff of Koalas 1.8.2's source against a 3.5.5 `pyspark.pandas` import |
| `pandas_on_spark_rules.py` | Six 3.3 → 3.4 pandas API on Spark rules (`PandasOnSpark*`), run only in a repo that uses pandas-on-Spark or Koalas, and each asking whether the frame came from `ps`/`ks` or plain pandas |
| `_cst.py` | Shared helpers: literals, one-hop name resolution, argument lookup, the reader chain's options |
| `sql.py` | `extract_sql(module)` — every `.sql(...)` call site, classified as literal, interpolated or dynamic, with the text of each literal; then every SQL statement *defined* in the file — a constant, registry entry, template or f-string whose text is a statement, placeholders filled — that is not already a call site's literal. Not a rule: plumbing that hands embedded SQL to a caller to lint, with the same Family K codes the Scala and Java paths use. `sql_definitions(module)` is the same search, as nodes, for detectors |

There are 40 detectors; 12 of them are `Fixer`s. Most are the Scala rules a PySpark job can hit,
ported with the Scala rule's id. Each one's claim was run against a real PySpark 3.5.5 session
first, and several match a narrower shape than their Scala rule because of it.

## Writing a rule

- **A rule that reports is a `Detector`**: set `rule_id = "..."` (a literal, so the ids can be
  read from the source without importing it), match in `visit_*`, call `self.report(node, message)`, and add the class
  to `DETECTORS`. The message is what the report shows for that call site, so say what this call
  does on 3.x and what to do. Use `self.function(call, "to_date")` for `pyspark.sql.functions`
  (qualified names, `import *` included), `self.string(node)` for a literal or a name bound once.
  A rule that reads SQL text overrides `check_sql(node, sql)`: it is called for every call's SQL
  arguments (`self.sql_text_args`) and for every statement defined in the file.
- **A rule that rewrites is a `Fixer`** (it is still in `DETECTORS`). In `leave_*`, decide on the
  *original* node; where the rewrite gives exactly 2.4's result, `return self.rewrite(original,
  new, message)`; elsewhere `self.report(...)` at the rule's own tier and return the node
  unchanged. Analysis and codegen run the same code, so the report's Tier 1 sites are exactly the
  ones codegen rewrites. Run the rewrite on Spark 2.4.8 and 3.5.5 first: the rewritten code on
  3.5.5 must give the original's 2.4.8 result.
- **Run the claim on Spark 3.5.5 before writing the matcher**, and add the shape that does *not*
  fail as a contrast test.
- **Ids.** A port of a Scala rule keeps the Scala rule's id verbatim
  (`DateTimeFormatPatternValidator`). A PySpark-only rule gets a descriptive PascalCase id
  (`SparkSessionBuilderConfigIgnored`). Never `PY<from>-<to>-<NNN>` — that space is upstream's.
- **Test it** in `tests/`: a landmine the rule must fire on and a contrast it must not
  (`test_detect.py`), and for a `Fixer` the rewritten code and the shapes it must leave alone
  (`test_fix.py`).

## Install and test

```bash
pip install ../pysparkler ./
python -m unittest discover -s tests     # 110 tests: a landmine and a contrast per rule, and every rewrite
```

`data/spark_core_config_keys_3_5_5.txt` lists every core config key Spark 3.5.5 registers; its
header says how it was dumped.
