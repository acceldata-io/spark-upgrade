"""The rules that rewrite (detector.Fixer), and the 2.4 -> 3.5 breaks added
with them. For each rewrite: the site analysis reports as Tier 1 is exactly
the site codegen rewrites, the rewritten file compiles, and the shapes it must
not touch are left alone. Every rewrite here was run on Spark 3.5.5 first --
the original fails or differs, the rewrite gives 2.4's result
(spark-migrate-cli PYSPARK.md SS2.7)."""
import textwrap
import unittest

import libcst as cst

from spark_upgrade_rules import detect, detector_ids, fix, fixer_ids

HEADER = "from pyspark.sql import Row, functions as F\n"


def analyze(code: str, rule: str) -> list[tuple[int, int | None, str]]:
    module = cst.parse_module(HEADER + textwrap.dedent(code))
    found, errors = detect(module, set(detector_ids()))
    assert not errors, errors
    return [(f["line"] - 1, f.get("tier"), f.get("confidence", "")) for f in found if f["rule"] == rule]


def rewritten(code: str, rule: str | None = None) -> str:
    module = cst.parse_module(HEADER + textwrap.dedent(code))
    out, _, errors = fix(module, {rule} if rule else set(fixer_ids()))
    assert not errors, errors
    compile(out, "<rewritten>", "exec")
    return out[len(HEADER):]


class RowFieldOrderTest(unittest.TestCase):
    rule = "RowKwargsFieldOrder"

    def test_unsorted_pure_kwargs_are_sorted(self):
        code = """
            a = Row(name=p[0].strip(), age=int(p[1]))
            b = Row(
                zone=z,
                amount=a,
            )
        """
        self.assertEqual(analyze(code, self.rule), [(2, 1, "high"), (3, 1, "high")])
        out = rewritten(code, self.rule)
        self.assertIn("a = Row(age=int(p[1]), name=p[0].strip())", out)
        self.assertIn("    amount=a,\n    zone=z,\n", out, "layout kept: each slot keeps its comma and line")

    def test_mapping_is_sorted_and_side_effects_are_reported(self):
        code = """
            a = Row(**rec.asDict())
            b = Row(b=next(it), a=1)
            c = Row(z=1, **rest)
        """
        self.assertEqual(analyze(code, self.rule), [(2, 1, "high"), (3, 3, "high"), (4, 3, "high")])
        out = rewritten(code, self.rule)
        self.assertIn("Row(**dict(sorted(rec.asDict().items())))", out)
        self.assertIn("Row(b=next(it), a=1)", out, "evaluation order is not changed behind a side effect")

    def test_contrasts(self):
        code = """
            a = Row(a=1, b=2)
            Person = Row("name", "age")
            p = Person("x", 1)
            class Row2: pass
            from sqlalchemy.engine import Row as SaRow
            s = SaRow(b=1, a=2)
        """
        self.assertEqual(analyze(code, self.rule), [], "sorted, positional, and not PySpark's Row")


class SharedParamSettersTest(unittest.TestCase):
    rule = "MlSharedParamSetters"
    IMPORTS = """
        from pyspark.ml import Transformer
        from pyspark.ml.param.shared import HasInputCol, HasOutputCol
        from pyspark.ml.util import DefaultParamsReadable
    """

    def test_missing_setters_are_added(self):
        code = self.IMPORTS + """
        class Lower(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable):
            def setOutputCol(self, value):
                return self._set(outputCol=value)

            def _transform(self, df):
                return df
        """
        self.assertEqual(analyze(code, self.rule), [(6, 1, "high")])
        out = rewritten(code, self.rule)
        self.assertIn("    def setInputCol(self, value):\n        return self._set(inputCol=value)\n", out)
        self.assertEqual(out.count("def setOutputCol"), 1, "a setter the class defines is kept, not duplicated")

    def test_unknown_base_is_reported_not_rewritten(self):
        code = self.IMPORTS + """
        class Mine(BaseStage, HasInputCol):
            pass
        class Plain(Transformer):
            pass
        """
        self.assertEqual(analyze(code, self.rule), [(6, 3, "medium")])
        self.assertNotIn("def setInputCol", rewritten(code, self.rule))


class RemovedApiTest(unittest.TestCase):
    rule = "RemovedPySparkApi"

    def test_renames(self):
        code = """
            from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer
            from pyspark.sql.utils import require_minimum_pandas_version
            import pyspark.ml.feature as feat
            enc = OneHotEncoderEstimator(inputCols=["a"], outputCols=["b"])
            enc2 = feat.OneHotEncoderEstimator(inputCols=["a"], outputCols=["b"])
        """
        self.assertEqual([x[:2] for x in analyze(code, self.rule)], [(2, 1), (3, 1), (6, 1)])
        out = rewritten(code, self.rule)
        self.assertIn("from pyspark.ml.feature import OneHotEncoder, StringIndexer", out)
        self.assertIn("from pyspark.sql.pandas.utils import require_minimum_pandas_version", out)
        self.assertIn("enc = OneHotEncoder(inputCols", out)
        self.assertIn("feat.OneHotEncoder(inputCols", out)

    def test_both_encoders_imported_keep_one_name(self):
        code = """
            from pyspark.ml.feature import OneHotEncoder, OneHotEncoderEstimator
            enc = OneHotEncoderEstimator(inputCols=["a"], outputCols=["b"])
        """
        out = rewritten(code, self.rule)
        self.assertIn("from pyspark.ml.feature import OneHotEncoder\n", out)

    def test_reported_removals(self):
        code = """
            from pyspark.ml.feature import OneHotEncoder
            from pyspark.ml.param.shared import DecisionTreeParams
            enc = OneHotEncoder(inputCol="i", outputCol="o")
            out = enc.transform(df)
            cost = model.computeCost(df)
            model.write().context(sqlContext).save(p)
            ok = Pipeline(stages=[enc]).fit(df)
        """
        self.assertEqual([x[:2] for x in analyze(code, self.rule)], [(3, 3), (5, 3), (6, 3), (7, 3)])

    def test_moved_names_are_reported_with_their_new_import(self):
        # Checked against 3.5.5: both live on, in pyspark.ml.wrapper and
        # pyspark.errors.exceptions.captured; neither at its 2.4 path.
        code = """
            from pyspark.ml.util import JavaPredictionModel
            from pyspark.sql.utils import capture_sql_exception
        """
        module = cst.parse_module(HEADER + textwrap.dedent(code))
        found = [f for f in detect(module, {self.rule})[0]]
        self.assertEqual([(f["line"] - 1, f["tier"]) for f in found], [(2, 3), (3, 3)])
        self.assertIn("from pyspark.ml.wrapper import JavaPredictionModel", found[0]["message"])
        self.assertIn("from pyspark.errors.exceptions.captured import capture_sql_exception", found[1]["message"])
        self.assertNotIn("removed", found[0]["message"] + found[1]["message"])


class AddMonthsSnapTest(unittest.TestCase):
    """Each rewritten shape was run on 3.5.5 and matched plain add_months on
    2.4.8 for 2019-02-28, '2019-04-30 10:00:00', 2020-02-29, 2019-02-15 and
    null, with n = 1 and -1 (spark-migrate-cli PYSPARK.md SS2.7)."""

    rule = "AddMonthsSnapDetect"

    def _found(self, code):
        module = cst.parse_module(HEADER + textwrap.dedent(code))
        found, _ = detect(module, {self.rule})
        return [(f["line"] - 1, f["rule"], f.get("tier")) for f in found]

    def test_safe_shapes_are_rewritten_with_the_calls_own_prefix(self):
        code = """
            a = df.withColumn("a", F.add_months("d", 1))
            b = df.select(F.add_months(F.col("d"), -1).alias("b"))
            c = df.filter(F.add_months(df.d, n) > F.lit("2020-01-01"))
            e = df.select((F.add_months(df["d"], 1) == x).name("e"))
            f = df.orderBy(F.add_months(F.to_timestamp(d, "yyyy-MM-dd HH:mm"), 1))
        """
        self.assertEqual(self._found(code), [(i, self.rule, 1) for i in range(2, 7)])
        out = rewritten(code, self.rule)
        self.assertIn('a = df.withColumn("a", F.when(F.to_date("d") == F.last_day("d"), F.last_day(F.add_months("d", 1)))'
                      '.otherwise(F.add_months("d", 1)))', out)
        self.assertIn('.otherwise(F.add_months(F.col("d"), -1)).alias("b")', out)
        self.assertIn("F.to_date(df.d) == F.last_day(df.d)", out)
        self.assertIn('F.to_date(df["d"])', out)
        self.assertIn('F.to_date(F.to_timestamp(d, "yyyy-MM-dd HH:mm"))', out)

    def test_a_generated_column_name_is_kept(self):
        # The rewrite renames an unaliased column (add_months(d, 1) -> CASE WHEN ...).
        code = """
            a = df.select(F.add_months("d", 1))
            b = F.add_months("d", 1)
            c = df.select(F.add_months("d", 1).cast("date"))
        """
        manual = "AddMonthsSnapDetect.Manual"
        self.assertEqual(self._found(code), [(2, manual, 3), (3, manual, 3), (4, manual, 3)])
        self.assertEqual(rewritten(code, self.rule), textwrap.dedent(code))

    def test_bare_import_is_extended(self):
        code = """
            from pyspark.sql.functions import add_months, col
            x = df.select(add_months(col("d"), 1).alias("x"))
        """
        out = rewritten(code, self.rule)
        self.assertIn("from pyspark.sql.functions import add_months, col, last_day, to_date, when\n", out)
        self.assertIn('when(to_date(col("d")) == last_day(col("d")), last_day(add_months(col("d"), 1)))'
                      '.otherwise(add_months(col("d"), 1)).alias("x")', out)
        already = """
            from pyspark.sql.functions import add_months, when, to_date, last_day
            x = df.withColumn("x", add_months("d", 1))
        """
        self.assertIn("import add_months, when, to_date, last_day\n", rewritten(already, self.rule))
        star = """
            from pyspark.sql.functions import *
            x = df.withColumn("x", add_months("d", 1))
        """
        self.assertIn("from pyspark.sql.functions import *\nx = df.withColumn(\"x\", when(", rewritten(star, self.rule))

    def test_unsafe_shapes_are_reported_manual(self):
        code = """
            from pyspark.sql.functions import add_months
            when = "2020-01-01"
            a = df.withColumn("a", F.add_months(F.expr("d + 1"), 1))
            b = df.withColumn("b", F.add_months("d", months()))
            c = df.withColumn("c", F.add_months(F.add_months("d", 1), 1))
            e = df.withColumn("e", add_months("d", 1))
            f = F.expr("add_months(d, 1)")
        """
        manual = "AddMonthsSnapDetect.Manual"
        self.assertEqual(self._found(code), [(4, manual, 3), (5, manual, 3), (6, self.rule, 1), (6, manual, 3),
                                             (7, manual, 3), (8, manual, 3)])
        out = rewritten(code, self.rule)
        self.assertIn('add_months("d", 1))\n', out, "a bare `when` bound to something else is not shadowed")
        self.assertIn('F.add_months(F.when(', out, "the inner call is rewritten, the outer reported")


class ReaderWriterFixTest(unittest.TestCase):
    def test_path_option_dropped_only_where_one_path_is_certain(self):
        rule = "PathOptionConflictDetect"
        code = """
            a = spark.read.format("csv").option("path", p).load("/in")
            b = (spark.read
                 .option("header", True)
                 .options(path=p, sep="|")
                 .csv(BASE + "/day"))
            c = df.write.option("path", p).save(q)
            d = spark.read.option("path", p).csv(paths)
            e = spark.read.option("path", p).load("/a", "/b")
        """
        self.assertEqual([x[:2] for x in analyze(code, rule)], [(2, 1), (3, None), (7, 1), (8, None), (9, None)])
        out = rewritten(code, rule)
        self.assertIn('a = spark.read.format("csv").load("/in")', out)
        # 2.4's csv(p) read the option's path AND p: dropping the option would read less.
        self.assertIn('.options(path=p, sep="|")', out)
        self.assertIn("c = df.write.save(q)", out)
        self.assertIn('d = spark.read.option("path", p).csv(paths)', out, "a name that may be a list keeps Tier 2")

    def test_empty_collection_cast(self):
        rule = "EmptyCollectionTypeDetect"
        code = """
            a = df.withColumn("n", F.array())
            b = df.withColumn("m", F.create_map().alias("m"))
            ok1 = F.coalesce(F.col("t"), F.array())
            ok2 = F.array().cast("array<int>")
        """
        self.assertEqual([x[:2] for x in analyze(code, rule)], [(2, 1), (3, 1)])
        out = rewritten(code, rule)
        self.assertIn('F.array().cast("array<string>")', out)
        self.assertIn('F.create_map().cast("map<string,string>").alias("m")', out)
        self.assertIn('ok2 = F.array().cast("array<int>")', out)

    def test_all_rewrites_chain_in_one_file(self):
        code = """
            from pyspark.ml.feature import OneHotEncoderEstimator
            r = Row(b=1, a=2)
            x = spark.read.option("path", p).load("/in")
            e = F.array()
        """
        out = rewritten(code)
        for expected in ("import OneHotEncoder\n", "Row(a=2, b=1)", 'spark.read.load("/in")', 'F.array().cast("array<string>")'):
            self.assertIn(expected, out)


class RowPrecedenceTest(unittest.TestCase):
    def test_row_star_star_of_an_expression_keeps_its_meaning(self):
        code = """
            from pyspark.sql import Row
            r = Row(**a or b)
            s = Row(**x if c else {})
        """
        out = rewritten(code, "RowKwargsFieldOrder")
        self.assertIn("Row(**dict(sorted((a or b).items())))", out)
        self.assertIn("Row(**dict(sorted((x if c else {}).items())))", out)


class KoalasReceiverTest(unittest.TestCase):
    def test_drop_axis_only_on_a_whole_frame(self):
        code = """
            import databricks.koalas as ks
            kdf = ks.read_csv("/in")
            a = kdf.drop("c")
            b = kdf.amount.drop(0)
            c = ks.Series([1, 2]).drop(0)
            d = kdf.columns.drop("x")
            e = kdf.index.drop([0])
        """
        out = rewritten(code, "PandasOnSparkDropAxis")
        self.assertIn('a = kdf.drop("c", axis=1)', out)
        for unchanged in ("kdf.amount.drop(0)", "ks.Series([1, 2]).drop(0)", 'kdf.columns.drop("x")', "kdf.index.drop([0])"):
            self.assertIn(unchanged, out)

    def test_koalas_accessor_only_on_a_koalas_frame(self):
        code = """
            import databricks.koalas as ks
            kdf = ks.read_csv("/in")
            a = kdf.koalas.apply_batch(f)
            b = args.koalas
        """
        out = rewritten(code, "KoalasToPandasOnSpark")
        self.assertIn("kdf.pandas_on_spark.apply_batch(f)", out)
        self.assertIn("b = args.koalas", out)


class EncoderImportTest(unittest.TestCase):
    def test_estimator_imported_on_its_own_line_beside_the_encoder(self):
        code = """
            from pyspark.ml.feature import OneHotEncoder
            from pyspark.ml.feature import OneHotEncoderEstimator
            enc = OneHotEncoderEstimator(inputCols=["a"], outputCols=["b"])
        """
        out = rewritten(code, "RemovedPySparkApi")
        self.assertNotIn("import OneHotEncoderEstimator", out)
        self.assertIn("enc = OneHotEncoder(inputCols", out)


class KoalasPortTest(unittest.TestCase):
    def test_imports_and_references(self):
        code = """
            import databricks.koalas as ks
            import databricks.koalas
            from databricks.koalas.config import set_option
            from databricks import koalas as kl
            from databricks.koalas.ml import corr
            y = databricks.koalas.DataFrame({"a": [1]})
            kdf = sdf.to_koalas()
            kdf.koalas.apply_batch(f)
        """
        found = analyze(code, "KoalasToPandasOnSpark")
        self.assertEqual([x[:2] for x in found], [(2, 1), (3, 1), (4, 1), (5, 1), (6, 3), (8, 1), (9, 1)])
        out = rewritten(code, "KoalasToPandasOnSpark")
        for expected in ("import pyspark.pandas as ks\n", "import pyspark.pandas\n", "from pyspark.pandas.config import set_option",
                         "from pyspark import pandas as kl", "from databricks.koalas.ml import corr",
                         "y = pyspark.pandas.DataFrame(", "kdf = sdf.pandas_api()", "kdf.pandas_on_spark.apply_batch(f)"):
            self.assertIn(expected, out)

    def test_removed_aliases_only_on_koalas_frames(self):
        code = """
            import databricks.koalas as ks
            def f(sdf, other):
                kdf = ks.DataFrame({"a": [1]})
                kdf.cache()
                kdf.spark_schema()
                kdf["a"].alias("b")
                t = kdf["a"].spark_type
                p = kdf.toPandas()
                sdf.cache()
                q = other.toPandas()
                r = spark.read.parquet("p").cache()
        """
        out = rewritten(code, "KoalasToPandasOnSpark")
        for expected in ("kdf.spark.cache()", "kdf.spark.schema()", 'kdf["a"].rename("b")', 'kdf["a"].spark.data_type',
                         "p = kdf.to_pandas()", "    sdf.cache()", "q = other.toPandas()", 'spark.read.parquet("p").cache()'):
            self.assertIn(expected, out)

    def test_no_koalas_no_change(self):
        code = """
            x = df.toPandas()
            y = s.alias("b")
        """
        self.assertEqual(analyze(code, "KoalasToPandasOnSpark"), [])

    def test_drop_axis(self):
        code = """
            import databricks.koalas as ks
            import pandas as pd
            def f(sdf, other):
                kdf = sdf.to_koalas()
                a = kdf.drop("c")
                b = kdf.drop(labels=["c"])
                ok1 = kdf["x"].drop(0)
                ok2 = kdf.drop("c", axis=1)
                ok3 = sdf.select("c").drop("c")
                ok4 = pd.DataFrame(x).drop("c")
                ok5 = spark.table("t").drop("c")
                low = other.drop("c")
        """
        self.assertEqual([x[:3] for x in analyze(code, "PandasOnSparkDropAxis")],
                         [(6, 1, "high"), (7, 1, "high"), (13, 3, "low")])
        out = rewritten(code, "PandasOnSparkDropAxis")
        self.assertIn('a = kdf.drop("c", axis=1)', out)
        self.assertIn('b = kdf.drop(labels=["c"], axis=1)', out)

    def test_sql_formatter(self):
        code = """
            import databricks.koalas as ks
            def q(kdf, floor):
                a = ks.sql("SELECT a FROM {kdf} WHERE a > {floor}")
                b = ks.sql("SELECT a FROM {kdf} WHERE a > {floor}", floor=floor)
                c = ks.sql("SELECT {x[0]}")
                ok1 = ks.sql("SELECT 1")
                ok2 = spark.sql("SELECT {x}")
        """
        self.assertEqual([x[:2] for x in analyze(code, "PandasOnSparkSqlFormatter")], [(4, 1), (5, 1), (6, 3)])
        out = rewritten(code, "PandasOnSparkSqlFormatter")
        self.assertIn('ks.sql("SELECT a FROM {kdf} WHERE a > {floor}", kdf=kdf, floor=floor)', out)
        self.assertIn('ks.sql("SELECT a FROM {kdf} WHERE a > {floor}", floor=floor, kdf=kdf)', out)

    def test_the_port_chains(self):
        code = """
            import databricks.koalas as ks
            def top(metrics):
                kdf = metrics.to_koalas()
                kdf = kdf.drop("returning")
                return ks.sql("SELECT * FROM {kdf}")
        """
        out = rewritten(code)
        for expected in ("import pyspark.pandas as ks", "metrics.pandas_api()", 'kdf.drop("returning", axis=1)',
                         'ks.sql("SELECT * FROM {kdf}", kdf=kdf)'):
            self.assertIn(expected, out)


class NewDetectorsTest(unittest.TestCase):
    def test_strict_parsing(self):
        code = """
            a = F.to_date("s", "yyyy-MM-dd")
            b = F.unix_timestamp("s")
            c = F.to_timestamp("s", FMT_FROM_CONFIG)
            d = spark.read.csv(p, schema=s, timestampFormat="yyyy-MM-dd HH:mm:ss")
            e = spark.sql("SELECT to_date(d, 'dd/MM/yyyy') FROM t")
            ok1 = F.to_date("s")
            ok2 = F.date_format("d", "yyyy-MM-dd")
            ok3 = F.to_date("s", "YYYY-MM-dd")
            ok4 = df.write.csv(p, dateFormat="yyyy-MM-dd")
            ok5 = F.unix_timestamp()
        """
        self.assertEqual(analyze(code, "DateTimeStrictParsingDetect"), [(n, None, "low") for n in (2, 3, 4, 5, 6)])
        self.assertEqual([x[0] for x in analyze(code, "DateTimeFormatPatternValidator")], [9], "never both on one site")

    def test_removed_and_ignored_configs(self):
        code = """
            a = SparkSession.builder.config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
            spark.conf.set("spark.sql.parquet.int64AsTimestampMillis", True)
            spark.sql("SET spark.sql.fromJsonForceNullableSchema=false")
            spark.conf.set("spark.sql.adaptive.minNumPostShufflePartitions", 8)
            ok1 = spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "false")
            ok2 = spark.conf.set("spark.sql.shuffle.partitions", 8)
        """
        self.assertEqual(analyze(code, "RemovedSqlConfigDetect"),
                         [(2, None, "high"), (3, None, "high"), (4, None, "high"), (5, None, "medium")])

    def test_command_output_schema(self):
        reads = """
            dbs = [r.databaseName for r in spark.sql("SHOW DATABASES").collect()]
            QUERY = "show tables in sales"
        """
        self.assertEqual(analyze(reads, "CommandOutputSchemaDetect"), [(2, None, "high"), (3, None, "high")])
        blind = """
            spark.sql("DESCRIBE DATABASE EXTENDED sales").show()
            ok = spark.sql("DESCRIBE TABLE sales.t")
            ok2 = spark.sql("SHOW PARTITIONS sales.t")
        """
        self.assertEqual(analyze(blind, "CommandOutputSchemaDetect"), [(2, None, "low")])

    def test_typing_namedtuple_class(self):
        code = """
            from typing import NamedTuple
            class Hit(NamedTuple):
                rule: str
            out = rdd.map(lambda r: Hit(r[0]))
        """
        self.assertEqual([x[0] for x in analyze(code, "NamedtupleCloudpickle")], [3])


if __name__ == "__main__":
    unittest.main()
