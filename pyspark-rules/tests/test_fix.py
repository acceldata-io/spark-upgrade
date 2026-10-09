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
        self.assertEqual([x[:2] for x in analyze(code, rule)], [(2, 1), (3, 1), (7, 1), (8, None), (9, None)])
        out = rewritten(code, rule)
        self.assertIn('a = spark.read.format("csv").load("/in")', out)
        self.assertIn('.options(sep="|")', out)
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
            x = spark.read.option("path", p).csv("/in")
            e = F.array()
        """
        out = rewritten(code)
        for expected in ("import OneHotEncoder\n", "Row(a=2, b=1)", 'spark.read.csv("/in")', 'F.array().cast("array<string>")'):
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
