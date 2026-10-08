# spark-upgrade-rules

Our own PySpark migration rules, for Spark 2.4 → 3.5. They run beside the vendored
`../pysparkler` engine, which is never edited, so that a pin bump never conflicts with our work.
`spark-migrate-cli`'s bridge script loads both sets and runs only the ids its registry names.

This mirrors the other two languages: `../scalafix` holds the Scala rules, `../openrewrite` the
Java recipes, and this package the Python rules. Tier, description and doc link live in
`spark-migrate-cli`'s `PyRuleRegistry`, not here.

## What is in it

| Module | What |
|---|---|
| `spark_upgrade_rules/__init__.py` | The contract with the bridge: `transformers()` (rewriting rules, none yet), `detector_ids()` and `detect(module, run_ids)` (the detectors below), `extract_sql(module)` |
| `detector.py` | `Detector`, the base of a detection-only rule: a LibCST visitor that calls `report(node, message, confidence)` and never edits the tree; and `run`, which runs the chosen ones over one file |
| `datetime_rules.py` | `DateTimeFormatPatternValidator`, `AddMonthsSnapDetect`, `InvalidTimeZoneIdDetect` — Family D |
| `function_rules.py` | `NegativeDecimalScaleDetect`, `DuplicateMapKeyLiteralDetect`, `MapTypeKeyInCreateMapDetect`, `EmptyCollectionTypeDetect`, `HashOnMapTypeDetect`, `SplitEmptyRegexDetect`, `GroupingIdTypeDetect` — Families E, F |
| `dataframe_rules.py` | `SelfJoinAmbiguousColumnDetect`, `NaFunctionsNameMatchDetect` — Family G |
| `config_rules.py` | `SetCommandSparkConfDetect` — `spark.conf.set` on a core config, against `data/spark_core_config_keys_3_5_5.txt` |
| `datasource_rules.py` | `PathOptionConflictDetect`, `JsonEmptyStringDetect`, `CsvBinaryTypeDetect`, `CsvBomMultilineDetect`, `MultiLineDatasetReadWarn` — Family H |
| `pyspark_guide_rules.py` | `ArrayTypeSchemaInference`, `NamedtupleCloudpickle` — the PySpark guide's own 3.3 → 3.4 core changes, no Scala rule to port |
| `pandas_on_spark_rules.py` | Six 3.3 → 3.4 pandas API on Spark rules (`PandasOnSpark*`), run only in a repo that uses pandas-on-Spark or Koalas, and each asking whether the frame came from `ps`/`ks` or plain pandas |
| `_cst.py` | Shared helpers: literals, one-hop name resolution, argument lookup, the reader chain's options |
| `sql.py` | `extract_sql(module)` — every `.sql(...)` call site, classified as literal, interpolated or dynamic, with the text of each literal. Not a rule: plumbing that hands embedded SQL to the JVM side, which lints it with the same 13 Family K codes the Scala and Java paths use |

The detectors are the Scala rules a PySpark job can hit, ported with the Scala rule's id. Each
one's claim was run against a real PySpark 3.5.5 session first, and several match a narrower
shape than their Scala rule because of it — `spark-migrate-cli/PYSPARK.md` §2.5 and §8.12.

## Writing a rule

- **A rule that reports is a `Detector`**: set `rule_id = "..."` (a literal — spark-migrate-cli's
  drift test reads it), match in `visit_*`, call `self.report(node, message)`, and add the class
  to `DETECTORS`. The message is what the report shows for that call site, so say what this call
  does on 3.x and what to do. Use `self.function(call, "to_date")` for `pyspark.sql.functions`
  (qualified names, `import *` included), `self.string(node)` for a literal or a name bound once.
- **A rule that rewrites subclasses PySparkler's `BaseTransformer`** and goes in
  `transformers()`; the bridge diffs its output.
- **Run the claim on Spark 3.5.5 before writing the matcher**, and add the shape that does *not*
  fail as a contrast test.
- **Ids.** A port of a Scala rule keeps the Scala rule's id verbatim
  (`DateTimeFormatPatternValidator`). A PySpark-only rule gets a descriptive PascalCase id
  (`SparkSessionBuilderConfigIgnored`). Never `PY<from>-<to>-<NNN>` — that space is upstream's.
- **Register it** in `spark-migrate-cli` — a port in `PySparkPortedRules` (the Scala tier), a
  PySpark-only rule in `PyRuleRegistry` (Tier 3) — with a fixture landmine and a must-not-fire
  contrast. An unregistered id is built but never run.

See `spark-migrate-cli/PYSPARK.md` §11.3, §11.4 and §11.7.

## Install and test

```bash
pip install ../pysparkler ./            # or: spark-migrate-cli/scripts/setup-pyspark.sh <venv>
python -m unittest discover -s tests     # 33 tests: a landmine and a contrast per detector
```

`data/spark_core_config_keys_3_5_5.txt` is copied in spark-migrate-cli
(`src/main/resources/spark/core-config-keys-3.5.5.txt`); its drift test fails if they differ.
