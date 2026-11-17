"""The Spark line the job migrates to, which a few rules read: what a 3.3
target has differs from 3.5's (the names `functions` binds, where an API
lives, the core config keys).

spark-migrate-cli passes it as SPARK_MIGRATE_TARGET_SPARK (`3.3` or `3.5`);
unset means 3.5, the only target there was before the variable existed."""
from __future__ import annotations

import os

RELEASES = {"3.3": "3.3.3", "3.5": "3.5.5"}

LINE = os.environ.get("SPARK_MIGRATE_TARGET_SPARK", "3.5").strip() or "3.5"
if LINE not in RELEASES:
    raise ValueError(f"SPARK_MIGRATE_TARGET_SPARK={LINE!r}: expected one of {', '.join(RELEASES)}")
RELEASE = RELEASES[LINE]


def at_least(line: str) -> bool:
    """The target line is `line` or later (`at_least("3.4")` is False on 3.3)."""
    return tuple(int(p) for p in LINE.split(".")) >= tuple(int(p) for p in line.split("."))
