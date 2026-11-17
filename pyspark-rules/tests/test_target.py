"""The rules whose behaviour depends on the target Spark line
(SPARK_MIGRATE_TARGET_SPARK, `spark_upgrade_rules.target`). The line is read
once, at import, so each target runs in its own interpreter."""
import json
import os
import subprocess
import sys
import textwrap
import unittest

PROBE = textwrap.dedent('''
    import json, libcst as cst
    from spark_upgrade_rules import detect, fix, target
    from spark_upgrade_rules.config_rules import CORE_KEYS
    from spark_upgrade_rules.star_import_rules import NEW_STAR_NAMES

    def found(code, rule):
        return [(f["line"], f.get("tier")) for f in detect(cst.parse_module(code), {rule})[0]]

    star = """since = "2020-01-01"
    from functools import reduce
    mode = "month"
    from pyspark.sql.functions import *
    print(since, mode, reduce)
    """
    keywords = """from pyspark.sql import functions as F
    c = F.regexp_replace(str="v", pattern="[0-9]", replacement="")
    a = F.lag("k", count=1)
    """
    moved = "from pyspark.sql.utils import capture_sql_exception\\n"
    conf = """spark.conf.set("spark.executor.maxNumFailures", 3)
    spark.conf.set("spark.executor.memory", "4g")
    """
    print(json.dumps({
        "line": target.LINE, "release": target.RELEASE,
        "star_names": len(NEW_STAR_NAMES),
        "star": [m for m in (f["message"] for f in detect(cst.parse_module(star), {"StarImportShadowsFilter"})[0])],
        "keywords_fixed": fix(cst.parse_module(keywords), {"RemovedPySparkApi"})[0],
        "moved": found(moved, "RemovedPySparkApi"),
        "conf": found(conf, "SetCommandSparkConfDetect"),
        "core_keys": len(CORE_KEYS),
    }))
''')


def probe(line: str | None) -> dict:
    env = {k: v for k, v in os.environ.items() if k != "SPARK_MIGRATE_TARGET_SPARK"}
    if line is not None:
        env["SPARK_MIGRATE_TARGET_SPARK"] = line
    out = subprocess.run([sys.executable, "-c", PROBE], env=env, capture_output=True, text=True, check=True)
    return json.loads(out.stdout.strip().splitlines()[-1])


class TargetLineTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.t33, cls.t35, cls.unset = probe("3.3"), probe("3.5"), probe(None)

    def test_unset_is_35(self):
        self.assertEqual(self.unset, self.t35)
        self.assertEqual((self.t35["line"], self.t35["release"]), ("3.5", "3.5.5"))
        self.assertEqual((self.t33["line"], self.t33["release"]), ("3.3", "3.3.3"))

    def test_star_import_names_follow_the_target(self):
        # 3.3.3's functions binds `since` (2.4.8's __all__ left it out) but not `mode` or `reduce`.
        self.assertEqual((self.t35["star_names"], self.t33["star_names"]), (255, 73))
        self.assertTrue(any("`mode`" in m for m in self.t35["star"]))
        self.assertFalse(any("`since`" in m for m in self.t35["star"]))
        self.assertTrue(any("`since`" in m for m in self.t33["star"]))
        self.assertFalse(any("`mode`" in m or "`reduce`" in m for m in self.t33["star"]))
        self.assertTrue(all("3.3.3" in m for m in self.t33["star"]))

    def test_regexp_replace_keyword_is_kept_on_33(self):
        # `str` became `string` in 3.4; lag/lead's `count` -> `offset` holds on both.
        self.assertIn('regexp_replace(string="v"', self.t35["keywords_fixed"])
        self.assertIn('regexp_replace(str="v"', self.t33["keywords_fixed"])
        for t in (self.t33, self.t35):
            self.assertIn('F.lag("k", offset=1)', t["keywords_fixed"])

    def test_exception_handler_moved_only_from_34(self):
        self.assertEqual(len(self.t35["moved"]), 1)
        self.assertEqual(self.t33["moved"], [])

    def test_core_keys_are_the_targets(self):
        # spark.executor.maxNumFailures arrived in 3.5: a core key there, unknown to 3.3.3.
        self.assertEqual((self.t35["core_keys"], self.t33["core_keys"]), (625, 596))
        self.assertEqual([line for line, _ in self.t35["conf"]], [1, 2])
        self.assertEqual([line for line, _ in self.t33["conf"]], [2])


if __name__ == "__main__":
    unittest.main()
