"""Every detector: a landmine that must fire and a contrast that must not.

The contrasts are the point. Each one is a shape a looser matcher would flag
and Spark 3.5.5 runs fine -- probed against a real 3.5.5 session (spark-migrate-cli
PYSPARK.md SS8.12) -- or a pandas call with the same method name.
"""
import textwrap
import unittest

import libcst as cst

from spark_upgrade_rules import detect, detector_ids
from spark_upgrade_rules.datetime_rules import check_pattern, zone_resolves

HEADER = "from pyspark.sql import functions as F\nfrom pyspark.sql.types import *\n"


def run(code: str, rule: str | None = None) -> list[dict]:
    module = cst.parse_module(HEADER + textwrap.dedent(code))
    found, errors = detect(module, set(detector_ids()))
    assert not errors, errors
    return [f for f in found if rule is None or f["rule"] == rule]


def lines(code: str, rule: str) -> list[int]:
    # HEADER is two lines; report lines as they appear in `code` (after dedent's leading newline).
    return [f["line"] - 2 for f in run(code, rule)]


class DefinedSqlTest(unittest.TestCase):
    """The SQL-text rules read SQL where it is defined, not only call literals."""

    def test_constants_registries_and_templates(self):
        code = """
            LOCAL = "SELECT from_utc_timestamp(ts, 'SAST') AS local_ts FROM cdr"
            QUERIES = {"cycle": "SELECT add_months(cycle_start, 1) AS cycle_end FROM accounts WHERE d = '{day}'"}
            LABEL = f"SELECT date_format(ts, 'YYYY-MM') AS m FROM {table}"
            OK = "SELECT from_utc_timestamp(ts, 'Africa/Johannesburg') FROM cdr"
        """
        self.assertEqual(lines(code, "InvalidTimeZoneIdDetect"), [2])
        self.assertEqual(lines(code, "AddMonthsSnapDetect.Manual"), [3])
        self.assertEqual(lines(code, "DateTimeFormatPatternValidator"), [4])

    def test_a_call_literal_is_reported_once_at_the_call(self):
        code = """
            Q = "SELECT add_months(d, 1) FROM t"
            spark.sql(Q)
        """
        self.assertEqual(lines(code, "AddMonthsSnapDetect.Manual"), [3])


class DateTimePatternTest(unittest.TestCase):
    rule = "DateTimeFormatPatternValidator"

    def test_spark_refuses_these_whatever_the_data(self):
        for pattern, parsing in [("YYYY-MM-dd", True), ("yyyy-ww", False), ("dd u", False), ("EEE, dd MMM", True),
                                 ("yyyy-MM-F", True), ("aa", False), ("ddd", True), ("MMMMM", False), ("yyyy#MM", False)]:
            verdict = check_pattern(pattern, parsing)
            self.assertIsNotNone(verdict, pattern)
            self.assertEqual(verdict[0], "high", pattern)

    def test_same_pattern_different_meaning(self):
        self.assertEqual(check_pattern("yyyy-MM-F", parsing=False)[0], "medium", "F formats, with a new meaning")
        self.assertEqual(check_pattern("dd/MM/yyyy hh:mm", parsing=True)[0], "medium")

    def test_patterns_3_5_5_reads_as_2_4_did_are_silent(self):
        for pattern in ["yyyy-MM-dd", "dd-MMM-yy", "MMM-yyyy", "yyMMdd_HHmmss", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
                        "yyyy-MM-dd hh:mm:ss a", "yyyy"]:
            self.assertIsNone(check_pattern(pattern, parsing=True), pattern)
        self.assertIsNone(check_pattern("dd/MM/yyyy hh:mm", parsing=False), "formatting with hh prints what 2.4 printed")
        self.assertIsNone(check_pattern("EEE yyyy", parsing=False), "E formats fine; only parsing with it fails")
        self.assertIsNone(check_pattern("yyyy-'YYYY'", parsing=True), "quoted text is not a pattern letter")

    def test_call_sites_options_and_sql_text(self):
        code = """
            FMT = "yyyy-MM-dd hh:mm:ss"
            a = F.to_timestamp(F.col("ts"), FMT)
            b = F.date_format("d", format="YYYY")
            c = spark.read.option("dateFormat", "yyyy-ww").csv(p)
            d = spark.read.csv(p, timestampFormat="dd/MM/yyyy hh:mm")
            e = df.selectExpr("to_date(s, 'YYYY-MM-dd') as d")
            f = spark.sql("select date_format(d, 'u') from t")
            g = F.from_json("j", schema, {"timestampFormat": "YYYY"})
            ok1 = F.to_date("d", "yyyy-MM-dd")
            ok2 = df.write.option("dateFormat", "yyyy-MM-dd").csv(p)
            ok3 = F.date_format("d", "hh:mm")
        """
        self.assertEqual(lines(code, self.rule), [3, 4, 5, 6, 7, 8, 9])

    def test_star_import_resolves_bare_names_unless_rebound(self):
        found = run("from pyspark.sql.functions import *\nx = to_date(c, 'YYYY')\n", self.rule)
        self.assertEqual(len(found), 1)
        self.assertEqual(run("def to_date(c, f): return c\nx = to_date(c, 'YYYY')\n", self.rule), [])


class TimeZoneTest(unittest.TestCase):
    rule = "InvalidTimeZoneIdDetect"

    def test_what_resolves_matches_spark(self):
        # Every one of these was checked against from_utc_timestamp on 3.5.5.
        for tz in ["UTC", "GMT+5", "UTC+5:30", "+05:30", "+5:30", "-0800", "EST", "PST", "IST", "Asia/Calcutta", "Etc/GMT+5", "Z"]:
            self.assertIsNot(zone_resolves(tz), False, tz)
        for tz in ["utc", "PDT", "US/Pacific-New", "America/Calcutta", "+19:00", "+530", "Asia/Kolkata ", ""]:
            self.assertIs(zone_resolves(tz), False, tz)

    def test_call_sites(self):
        code = """
            a = F.from_utc_timestamp("ts", "PDT")
            spark.conf.set("spark.sql.session.timeZone", "utc")
            b = spark.sql("select to_utc_timestamp(ts, 'CEST') from t")
            ok1 = F.from_utc_timestamp("ts", "America/Los_Angeles")
            ok2 = SparkSession.builder.config("spark.sql.session.timeZone", "UTC")
        """
        self.assertEqual(lines(code, self.rule), [2, 3, 4])


class DateTimeStringComparisonTest(unittest.TestCase):
    rule = "DateTimeStringComparisonDetect"

    def test_pairs_that_change_on_a_column(self):
        code = """
            a = df.filter(F.col("ts") > "2020-01-01")
            b = df.where(df.d == "2020-1-1")
            c = F.col("d") <= F.lit("2020-01-01 10:00:00")
            d = df.filter("2020-01-01" >= df["ts"])
            e = F.to_date("s").between("2020-01-01", "2020-01-31")
            f = df.d.eqNullSafe("2020-01-01T10:00")
            g = F.when(df.ts <= "2020-01-31", 1)
            CUTOFF = "2020-1-1"
            h = F.col("d") < CUTOFF
        """
        self.assertEqual(lines(code, self.rule), [2, 3, 4, 5, 6, 7, 8, 10])

    def test_pairs_that_do_not_change_and_python_strings(self):
        code = """
            ok1 = df.filter(F.col("dt") == "2020-01-01")
            ok2 = df.filter((F.col("ts") >= "2020-01-01") & (F.col("ts") < "2020-02-01"))
            ok3 = F.col("d").isin("2020-1-1")
            ok4 = run_date > "2020-01-01"
            ok5 = rdd.filter(lambda r: r.ts > "2020-01-01")
            ok6 = df.filter(F.col("month") > "2020-01")
            ok7 = pdf["d"].between("2020-01-01", "2020-01-31")
        """
        self.assertEqual(lines(code, self.rule), [])


class FunctionRulesTest(unittest.TestCase):
    def test_add_months(self):
        code = """
            a = F.add_months("d", 1)
            b = F.expr("add_months(d, 1)")
            ok = F.date_add("d", 1)
        """
        self.assertEqual(lines(code, "AddMonthsSnapDetect.Manual"), [2, 3], "a generated name, and SQL text, are reported; "
                                                                           "rewrites: test_fix.AddMonthsSnapTest")

    def test_negative_decimal_scale(self):
        code = """
            a = c.cast(DecimalType(38, -2))
            b = DecimalType(precision=10, scale=-1)
            ok = DecimalType(38, 2)
        """
        self.assertEqual(lines(code, "NegativeDecimalScaleDetect"), [2, 3])

    def test_duplicate_map_keys(self):
        code = """
            a = F.create_map(F.lit("source"), F.lit(1), F.lit("source"), F.col("x"))
            b = F.create_map("k", "v", "k", "w")
            ok1 = F.create_map(F.lit("a"), F.lit(1), F.lit("b"), F.lit(1))
            ok2 = F.create_map(F.lit(1), F.lit(1), F.lit("1"), F.lit(1))
        """
        self.assertEqual(lines(code, "DuplicateMapKeyLiteralDetect"), [2, 3])

    def test_map_as_key_empty_collections_hash_on_map(self):
        code = """
            a = F.create_map(F.create_map(F.lit("a"), F.lit(1)), F.lit(1))
            b = F.map_from_arrays(F.array(F.create_map(k, v)), F.array(F.lit(1)))
            c = F.array()
            d = F.create_map([])
            e = F.hash(F.col("id"), F.create_map(k, v))
            ok1 = F.array(F.lit(1))
            ok2 = F.array(*cols)
            ok3 = F.hash(F.col("m"))
        """
        self.assertEqual(lines(code, "MapTypeKeyInCreateMapDetect"), [2, 3])
        self.assertEqual(lines(code, "EmptyCollectionTypeDetect"), [4, 5])
        self.assertEqual(lines(code, "HashOnMapTypeDetect"), [6])

    def test_empty_collection_widened_by_its_context(self):
        # Probed on 3.5.5: each `ok` widens to the typed side and writes to
        # Parquet; each landmine stays array<void>/map<void,void>.
        code = """
            ok1 = F.coalesce(F.col("tags"), F.array())
            ok2 = F.when(F.col("n") > 0, F.col("tags")).otherwise(F.array())
            ok3 = F.when(F.col("n") > 5, F.array()).otherwise(F.col("tags"))
            ok4 = F.array_union("tags", F.array())
            ok5 = F.coalesce(F.col("attrs"), F.create_map())
            bad1 = F.when(F.col("n") > 0, F.array())
            bad2 = F.coalesce(F.array())
            bad3 = df.withColumn("notes", F.array())
        """
        self.assertEqual(lines(code, "EmptyCollectionTypeDetect"), [7, 8, 9])

    def test_split_and_grouping_id(self):
        code = """
            a = F.split("s", "")
            ok = F.split("s", ",")
            g = df.cube("c").agg(F.grouping_id())
        """
        self.assertEqual(lines(code, "SplitEmptyRegexDetect.Manual"), [2], "a generated name: not rewritten (test_fix)")
        self.assertEqual(lines(code, "GroupingIdTypeDetect"), [4])


class DataFrameRulesTest(unittest.TestCase):
    rule = "SelfJoinAmbiguousColumnDetect"

    def test_the_shapes_3_5_5_rejects(self):
        code = """
            def f(df):
                df2 = df.filter("a > 0")
                a = df.join(df2, df.a > df2.a)
                b = df.join(df2, (df.a == df2.a) & (df.b > df2.b))
                c = df.join(df2, df["a"] == df2["b"])
                d = df.join(df, df.a == df.a).groupBy(df.a).count()
                e = df.join(df2, "a").select(df2.b)
        """
        self.assertEqual(lines(code, self.rule), [4, 5, 6, 7, 8])

    def test_the_shapes_3_5_5_resolves(self):
        code = """
            def f(df, other):
                df2 = df.filter("a > 0")
                ok1 = df.join(df, "a")
                ok2 = df.join(df2, df.a == df2.a)
                ok3 = df.join(df2, df.a != df2.a)
                ok4 = df.alias("x").join(df2.alias("y"), F.col("x.a") > F.col("y.a"))
                ok5 = df.join(other, df.a > other.a)
                ok6 = df.join(df2, F.col("a") > 1)
                ok8 = df.join(df2, ["a", "b"], "left").drop(df2.b)
                ok9 = df.join(df, "customer_id").groupBy("customer_id").count()
            def g(df):
                df2 = df.filter("a > 0")
                df2 = df2.cache()
                ok7 = df.join(df2, df.a > df2.a)
        """
        self.assertEqual(run(code, self.rule), [], "ok7: df2 is rebound, so its lineage is not followed")

    def test_na_replace_with_names_only(self):
        # Probed: replace skipped a missing or nested name on 2.4.8 and throws on
        # 3.5.5; fill/fillna failed on a missing name on both, so they are not flagged.
        code = """
            d = df.na.replace(1, 5, "x")
            e = df.replace("a", "b", ["col"])
            f = df.na.replace("x", "y", ["st.p"])
            ok1 = df.na.fill(0, ["total", "count"])
            ok2 = df.fillna(value=0, subset=cols)
            ok3 = df.na.fill({"a.b": 0})
            ok4 = "a.b".replace(".", "_")
            ok5 = s.replace("a", "b", 1)
            ok6 = df.na.replace(1, 5)
        """
        found = run(code, "NaFunctionsNameMatchDetect")
        self.assertEqual([f["line"] - 2 for f in found], [2, 3, 4])
        self.assertIn("`st.p`", found[2]["message"], "a dotted name is told what it means")


class ConfigRuleTest(unittest.TestCase):
    def test_only_keys_3_5_5_registers_as_core(self):
        code = """
            spark.conf.set("spark.executor.memory", "4g")
            self.spark.conf.set("spark.yarn.queue", "etl")
            ok1 = spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
            ok2 = spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            ok3 = spark.conf.set("spark.default.parallelism", "8")
            ok4 = conf.set("spark.executor.memory", "4g")
            ok5 = self.conf.set("spark.executor.memory", "4g")
            ok6 = SparkSession.builder.config("spark.executor.memory", "4g")
        """
        self.assertEqual(lines(code, "SetCommandSparkConfDetect"), [2, 3])


class DataSourceRulesTest(unittest.TestCase):
    def test_path_option(self):
        code = """
            a = spark.read.format("csv").option("path", p).load(q)
            b = spark.read.option("path", p).csv(q)
            c = spark.read.options(path=p).json(q)
            d = df.write.option("path", p).save(q)
            ok1 = spark.read.format("csv").load(path=p)
            ok2 = spark.read.option("path", p).load()
            ok3 = df.write.option("path", p).saveAsTable("t")
        """
        self.assertEqual(lines(code, "PathOptionConflictDetect"), [2, 3, 4, 5])

    def test_json_empty_string_only_where_it_shows(self):
        code = """
            a = spark.read.schema(s).option("mode", "FAILFAST").json(p)
            b = spark.read.json(p, schema=s, mode="DROPMALFORMED")
            c = spark.read.schema("n INT, _corrupt_record STRING").json(p)
            ok1 = spark.read.schema(s).json(p)
            ok2 = spark.read.option("mode", "FAILFAST").json(p)
        """
        found = run(code, "JsonEmptyStringDetect")
        self.assertEqual([(f["line"] - 2, f["confidence"]) for f in found], [(2, "high"), (3, "high"), (4, "medium")])

    def test_store_assignment_on_writes_into_a_table_schema(self):
        code = """
            df.write.insertInto("billing.usage_daily")
            df.write.mode("append").insertInto("billing.usage_daily", overwrite=True)
            df.write.mode("append").saveAsTable("billing.usage")
            df.write.saveAsTable("billing.usage", mode="append")
            df.write.format("parquet").mode(write_mode).saveAsTable("billing.usage")
            ok1 = df.write.saveAsTable("billing.fresh")
            ok2 = df.write.mode("overwrite").saveAsTable("billing.usage")
            ok3 = df.write.saveAsTable("billing.usage", "parquet", "overwrite")
            ok4 = df.write.saveAsTable("billing.usage", mode=None)
        """
        found = run(code, "StoreAssignmentPolicyDetect")
        self.assertEqual([(f["line"] - 2, f["confidence"]) for f in found],
                         [(2, "medium"), (3, "medium"), (4, "medium"), (5, "medium"), (6, "low")])

    def test_save_as_table_path_in_the_default_mode(self):
        code = """
            df.write.option("path", p).saveAsTable("t")
            df.write.format("parquet").options(path=p).mode("errorifexists").saveAsTable("t")
            df.write.saveAsTable("t", path=p)
            df.write.option("path", p).mode(write_mode).saveAsTable("t")
            ok1 = df.write.option("path", p).mode("overwrite").saveAsTable("t")
            ok2 = df.write.saveAsTable("t", "parquet", "append", path=p)
            ok3 = df.write.saveAsTable("t")
        """
        found = run(code, "SaveAsTablePathDetect")
        self.assertEqual([(f["line"] - 2, f["confidence"]) for f in found],
                         [(2, "high"), (3, "high"), (4, "high"), (5, "low")])

    def test_csv_binary_schema_one_hop(self):
        code = """
            SCHEMA = StructType([StructField("id", LongType()), StructField("blob", BinaryType())])
            a = spark.read.schema(SCHEMA).csv(p)
            b = spark.read.csv(p, schema="id BIGINT, payload BINARY")
            ok1 = spark.read.schema(SCHEMA).parquet(p)
            ok2 = spark.read.csv(p, schema="id BIGINT, binary_flag STRING")
        """
        self.assertEqual(lines(code, "CsvBinaryTypeDetect"), [3, 4])

    def test_csv_multiline_without_encoding(self):
        code = """
            a = spark.read.csv(f, sep=",", header=True, multiLine=True, inferSchema=True)
            b = spark.read.option("multiLine", "true").csv(f)
            ok1 = spark.read.option("multiLine", True).option("encoding", "UTF-16").csv(f)
            ok2 = spark.read.csv(f, multiLine=False)
            ok3 = pd.read_csv(f)
        """
        self.assertEqual(lines(code, "CsvBomMultilineDetect"), [2, 3])
        # multiLine=False reads line by line: not multi-line at all.
        self.assertEqual(lines(code, "MultiLineDatasetReadWarn"), [2, 3, 4])


class PySparkGuide34Test(unittest.TestCase):
    """The PySpark guide's 3.3 -> 3.4 hop (PYSPARK.md G14)."""

    def test_array_inference_decidable_from_literal_rows(self):
        code = """
            a = spark.createDataFrame([(1, [1, 1.5])], ["id", "xs"])
            ROWS = [Row(id=1, tags=[{"k": 1}, {"k": "x"}])]
            b = spark.createDataFrame(ROWS)
            ok1 = spark.createDataFrame([(1, [1, 2]), (2, [None, 3])], ["id", "xs"])
            ok2 = spark.createDataFrame([(1, "a")], ["id", "s"])
            ok3 = spark.createDataFrame([(1, [1, 1.5])], "id int, xs array<double>")
            ok4 = spark.read.json(p)
        """
        found = run(code, "ArrayTypeSchemaInference")
        self.assertEqual([(f["line"] - 2, f["confidence"]) for f in found], [(2, "high"), (4, "high")])

    def test_array_inference_unseen_data_is_low(self):
        code = """
            a = spark.createDataFrame(rows)
            b = spark.createDataFrame(list(aliases.items()), ["raw", "alias"])
            c = rdd.map(lambda r: (r.id, r.tags)).toDF(["id", "tags"])
            ok1 = spark.createDataFrame(df.rdd, df.schema)
            ok2 = spark.createDataFrame(rows, schema=SCHEMA)
        """
        found = run(code, "ArrayTypeSchemaInference")
        self.assertEqual([(f["line"] - 2, f["confidence"]) for f in found], [(2, "low"), (3, "low")])
        # An RDD's first row is not restored by the legacy flag on 3.5.5: its own id.
        self.assertEqual(lines(code, "ArrayTypeSchemaInference.Rdd"), [4])

    def test_array_inference_skips_dataframe_renames(self):
        code = """
            a = df.toDF(*cols)
            b = df.toDF("id", "name")
            c = rdd.toDF()
            d = rdd.toDF(["id", "tags"], 0.5)
            e = spark.createDataFrame(sc.parallelize(rows))
        """
        self.assertEqual(run(code, "ArrayTypeSchemaInference"), [])
        found = run(code, "ArrayTypeSchemaInference.Rdd")
        self.assertEqual([(f["line"] - 2, f["confidence"]) for f in found], [(4, "low"), (5, "low"), (6, "low")])

    def test_array_inference_reads_literal_rows_through_parallelize(self):
        # Rows the rule can see are decided: mixed is reported high (must fix), uniform not at all.
        code = """
            a = sc.parallelize([("a", [1, 2.5])]).toDF(["k", "vs"])
            b = spark.createDataFrame(sc.parallelize([("a", [1, 2])]))
            c = sc.parallelize(rows).toDF()
        """
        found = run(code, "ArrayTypeSchemaInference.Rdd")
        self.assertEqual([(f["line"] - 2, f["confidence"]) for f in found], [(2, "high"), (4, "low")])

    def test_array_inference_skips_a_dataframes_own_rows(self):
        # A typed column holds one element type, so its values infer the same under either
        # rule (the 2026-10-16 user trial's must-fix noise on marketing-pyspark).
        code = """
            a = df.rdd.map(lambda r: (r[0], r[2])).toDF(["campaign", "opens"])
            b = spark.createDataFrame(df.rdd)
            c = df.rdd.filter(lambda r: r.ok).map(lambda r: (r.id, r["tags"])).toDF()
            d = df.rdd.map(lambda r: r.tags).toDF()
            e = df.rdd.map(lambda r: (r.id, [r.a, r.b])).toDF()
            f = df.rdd.map(lambda r: (r.id, r.tags + [1.5])).toDF()
            g = other.map(lambda r: (r[0], r[1])).toDF()
        """
        found = run(code, "ArrayTypeSchemaInference.Rdd")
        self.assertEqual([f["line"] - 2 for f in found], [6, 7, 8], "a list built from the values, or rows of unknown origin")

    def test_array_inference_reads_a_name_bound_in_the_function(self):
        code = """
            def test_scalar(spark):
                data = [{"id": 1, "name": "a"}]

                df = spark.createDataFrame(data)

            def test_mixed(spark):
                data = [(1, [1, 1.5])]

                df = spark.createDataFrame(data, ["id", "xs"])

            def test_param(spark, data):
                df = spark.createDataFrame(data)
        """
        found = run(code, "ArrayTypeSchemaInference")
        self.assertEqual([(f["line"] - 2, f["confidence"]) for f in found], [(10, "high"), (13, "low")])

    def test_namedtuple_only_where_closures_ship(self):
        ships = "from collections import namedtuple\nP = namedtuple('P', ['a'])\nx = rdd.map(lambda i: P(i))\n"
        self.assertEqual([f["line"] - 2 for f in run(ships, "NamedtupleCloudpickle")], [2])
        driver_only = "from collections import namedtuple\nAppConfig = namedtuple('AppConfig', ['path'])\n"
        self.assertEqual(run(driver_only, "NamedtupleCloudpickle"), [], "a config tuple on the driver is not shipped")
        named = ("from collections import namedtuple\nHit = namedtuple('Hit', ['a'])\n"
                 "def hits(rows):\n    for r in rows:\n        yield Hit(r.a)\nout = df.rdd.mapPartitions(hits)\n")
        self.assertEqual([f["line"] - 2 for f in run(named, "NamedtupleCloudpickle")], [2], "a named function ships too")
        mapping = "from collections import namedtuple\nP = namedtuple('P', ['a'])\nCODES = {}\nx = series.map(CODES)\n"
        self.assertEqual(run(mapping, "NamedtupleCloudpickle"), [], "a dict passed to pandas .map ships nothing")
        local = "from collections import namedtuple\ndef f(rdd):\n    P = namedtuple('P', ['a'])\n    return rdd.map(lambda i: P(i))\n"
        self.assertEqual(run(local, "NamedtupleCloudpickle"), [], "a function-local namedtuple is pickled by value")

    def test_pandas_on_spark_shapes_and_provenance(self):
        code = """
            import pyspark.pandas as ps
            import databricks.koalas as ks
            import pandas as pd
            def f(sdf, other):
                psdf = sdf.pandas_api()
                kdf = ks.read_csv(p)
                kdf = kdf.fillna(0)
                pdf = sdf.toPandas()
                a = psdf.groupby("g").head(-1)
                b = kdf.groupby("g").apply(lambda g: g)
                c = psdf.index.insert(10, 4)
                d = psdf["v"].mode()
                e = kdf["v"].astype("category")
                h = ps.concat([psdf, psdf], sort=True)
                u = other.groupby("g").tail(-2)
                ok1 = pdf.groupby("g").head(-1)
                ok2 = pdf["v"].mode()
                ok3 = psdf.groupby("g").head(2)
                ok4 = sdf.write.mode("overwrite")
                ok5 = pd.concat([pdf, pdf], sort=True)
                ok6 = psdf.groupby("g").apply(typed)
            def typed(pdf) -> ps.DataFrame[int]:
                return pdf
        """
        found = run(code)
        got = [(f["rule"].replace("PandasOnSpark", ""), f["line"] - 2, f.get("confidence")) for f in found
               if f["rule"].startswith("PandasOnSpark")]
        self.assertEqual(got, [
            ("GroupByHeadTailNegative", 10, "medium"), ("GroupByApplyInference", 11, "medium"),
            ("IndexInsertBounds", 12, "medium"), ("SeriesModeName", 13, "medium"), ("AstypeCategory", 14, "medium"),
            ("ConcatSort", 15, "medium"), ("GroupByHeadTailNegative", 16, "low"),
        ])


class ContractTest(unittest.TestCase):
    def test_ids_are_unique_and_only_requested_ones_run(self):
        ids = detector_ids()
        self.assertEqual(len(ids), len(set(ids)))
        module = cst.parse_module(HEADER + "a = F.add_months('d', 1)\nb = F.split('s', '')\n")
        found, _ = detect(module, {"SplitEmptyRegexDetect"})
        self.assertEqual([f["rule"] for f in found], ["SplitEmptyRegexDetect.Manual"])

    def test_a_detector_that_raises_is_an_error_record_not_a_crash(self):
        from spark_upgrade_rules import detector as d

        class Broken(d.Detector):
            rule_id = "Broken"

            def visit_Call(self, node):
                raise RuntimeError("boom")

        found, errors = d.run(cst.parse_module("f()\n"), [Broken], {"Broken"})
        self.assertEqual((found, errors), ([], [{"rule": "Broken", "error": "boom"}]))


if __name__ == "__main__":
    unittest.main()
