import textwrap
import unittest

import libcst as cst

from spark_upgrade_rules import extract_sql, transformers


def sql_in(code: str) -> list[dict]:
    return extract_sql(cst.parse_module(textwrap.dedent(code)))


class ExtractSqlTest(unittest.TestCase):
    def test_literals_in_every_string_form(self):
        found = sql_in(
            '''
            spark.sql("SELECT 1")
            sqlContext.sql("""SELECT a
                FROM t""")
            self.spark.sql("SELECT " "b FROM t")
            '''
        )
        self.assertEqual([f["kind"] for f in found], ["literal"] * 3)
        self.assertEqual(found[0], {"line": 2, "kind": "literal", "text": "SELECT 1"})
        self.assertEqual(found[1]["text"], "SELECT a\n    FROM t")
        self.assertEqual(found[2]["text"], "SELECT b FROM t")

    def test_interpolated_forms(self):
        found = sql_in(
            '''
            spark.sql(f"SELECT * FROM {table}")
            spark.sql("SELECT * FROM %s" % table)
            spark.sql("SELECT * FROM {}".format(table))
            spark.sql("SELECT " f"{col} FROM t")
            '''
        )
        sites = [f for f in found if f["kind"] != "defined"]
        self.assertEqual([f["kind"] for f in sites], ["interpolated"] * 4)
        self.assertTrue(all("text" not in f for f in sites))
        # The template itself is still SQL: linted with its placeholders filled.
        self.assertEqual([f["text"] for f in found if f["kind"] == "defined"], [
            "SELECT * FROM __param__", "SELECT * FROM __param__", "SELECT * FROM __param__", "SELECT __param__ FROM t"])

    def test_dynamic_forms(self):
        found = sql_in(
            '''
            spark.sql(build_query())
            spark.sql("SELECT * FROM " + table)
            spark.sql(query)
            '''
        )
        self.assertEqual([f["kind"] for f in found], ["dynamic"] * 3 + ["defined"])

    def test_a_name_bound_once_to_a_literal_resolves_one_hop(self):
        found = sql_in(
            '''
            DAILY = """SELECT count(*) FROM orders"""
            def run(spark):
                return spark.sql(DAILY)
            '''
        )
        self.assertEqual(found, [{"line": 4, "kind": "literal", "text": "SELECT count(*) FROM orders"}])

    def test_a_name_bound_twice_or_to_a_non_literal_does_not_resolve(self):
        found = sql_in(
            '''
            q = "SELECT 1"
            if cond:
                q = "SELECT 2"
            spark.sql(q)
            r = make()
            spark.sql(r)
            def f(p):
                spark.sql(p)
            '''
        )
        self.assertEqual([f["kind"] for f in found], ["dynamic"] * 3)

    def test_sql_is_found_where_it_is_defined(self):
        found = sql_in(
            '''
            """Runs SELECT statements from the registry."""
            QUERIES = {
                "daily": """SELECT msisdn, count(c.*) FROM cdr c GROUP BY msisdn""",
                "label": "daily usage",
            }
            TABLES = ["CREATE TABLE t (c CHAR(3)) USING parquet", "not sql"]
            TEMPLATE = "INSERT OVERWRITE TABLE {table} PARTITION (dt='{day}') SELECT * FROM staged"
            OLD = "SELECT * FROM t WHERE d = '$day' AND n = %(n)s"
            USED = "SELECT a FROM used_here"
            def run(spark, name, day):
                log.info("select rows from %s", name)
                spark.sql(USED)
                spark.sql(QUERIES[name])
                spark.sql(f"select a from {name} where d = '{day}'")
                raise ValueError("SELECT failed FROM registry")
            '''
        )
        defined = [(f["line"], f["text"]) for f in found if f["kind"] == "defined"]
        self.assertEqual(defined, [
            (4, "SELECT msisdn, count(c.*) FROM cdr c GROUP BY msisdn"),
            (7, "CREATE TABLE t (c CHAR(3)) USING parquet"),
            (8, "INSERT OVERWRITE TABLE __param__ PARTITION (dt='__param__') SELECT * FROM staged"),
            (9, "SELECT * FROM t WHERE d = '__param__' AND n = __param__"),
            (15, "select a from __param__ where d = '__param__'"),
        ], "USED is linted at its call site, once; prose, logging and exception text are not SQL")

    def test_not_a_sql_call(self):
        self.assertEqual(sql_in("spark.read.sql_table('x')\nsql('SELECT 1')\nspark.sql()\n"), [])

    def test_the_package_exposes_a_fresh_transformer_list(self):
        self.assertIsInstance(transformers(), list)
        self.assertIsNot(transformers(), transformers())


if __name__ == "__main__":
    unittest.main()
