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
        self.assertEqual([f["kind"] for f in found], ["interpolated"] * 4)
        self.assertTrue(all("text" not in f for f in found))

    def test_dynamic_forms(self):
        found = sql_in(
            '''
            spark.sql(build_query())
            spark.sql("SELECT * FROM " + table)
            spark.sql(query)
            '''
        )
        self.assertEqual([f["kind"] for f in found], ["dynamic"] * 3)

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

    def test_not_a_sql_call(self):
        self.assertEqual(sql_in("spark.read.sql_table('x')\nsql('SELECT 1')\nspark.sql()\n"), [])

    def test_the_package_exposes_a_fresh_transformer_list(self):
        self.assertIsInstance(transformers(), list)
        self.assertIsNot(transformers(), transformers())


if __name__ == "__main__":
    unittest.main()
