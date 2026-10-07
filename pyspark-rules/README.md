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
| `spark_upgrade_rules/__init__.py` | `transformers()` — every rule in this package, for the bridge to merge with PySparkler's |
| `spark_upgrade_rules/sql.py` | `extract_sql(module)` — every `.sql(...)` call site, classified as literal, interpolated or dynamic, with the text of each literal. Not a rule: plumbing that hands embedded SQL to the JVM side, which lints it with the same 13 Family K codes the Scala and Java paths use |

## Writing a rule

- **Subclass PySparkler's `StatementLineCommentWriter`** (or `BaseTransformer` for a rewrite),
  so a rule emits the same trailing `# <id>: ...` comment and goes through the bridge's
  diff-based hint/transformation classification unchanged.
- **Ids.** A port of a Scala rule keeps the Scala rule's id verbatim
  (`DateTimeFormatPatternValidator`). A PySpark-only rule gets a descriptive PascalCase id
  (`SparkSessionBuilderConfigIgnored`). Never `PY<from>-<to>-<NNN>` — that space is upstream's.
- **Register it** in `spark-migrate-cli`'s `PyRuleRegistry`, at Tier 3, with a fixture landmine
  and a must-not-fire contrast. An unregistered id is built but never run.

See `spark-migrate-cli/PYSPARK.md` §11.3, §11.4 and §11.7.

## Install and test

```bash
pip install ../pysparkler ./            # or: spark-migrate-cli/scripts/setup-pyspark.sh <venv>
python -m unittest discover -s tests
```
