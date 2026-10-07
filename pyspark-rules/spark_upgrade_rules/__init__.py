"""Our own PySpark migration rules, run beside the vendored PySparkler.

`transformers()` is the whole contract with spark-migrate-cli's bridge script:
it merges these with `PySparkler(...).transformers` and runs only the ids
spark-migrate-cli's PyRuleRegistry names. See README.md for the conventions.
"""
from spark_upgrade_rules.sql import extract_sql

__all__ = ["extract_sql", "transformers"]


def transformers() -> list:
    """Every rule in this package, freshly constructed -- transformers are
    visitors and may keep state between visits, so callers build a new list
    per file."""
    return []
