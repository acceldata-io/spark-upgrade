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

    # The breaks below were each run on 2.4.8 and fail on 3.5.5; each rewrite
    # was run on 3.5.5 and gives 2.4.8's values.
    def _found(self, code):
        module = cst.parse_module(HEADER + textwrap.dedent(code))
        return [(f["line"] - 1, f["tier"], f["message"]) for f in detect(module, {self.rule})[0]]

    def test_serialized_storage_levels(self):
        code = """
            import pyspark
            from pyspark import StorageLevel
            from pyspark.storagelevel import StorageLevel as SL
            df.persist(StorageLevel.MEMORY_AND_DISK_SER)
            rdd.persist(pyspark.StorageLevel.MEMORY_ONLY_SER_2)
            x = SL.MEMORY_ONLY_SER
            ok = StorageLevel.MEMORY_AND_DISK
            other = Config.MEMORY_AND_DISK_SER
        """
        found = self._found(code)
        self.assertEqual([f[:2] for f in found], [(5, 1), (6, 1), (7, 1)])
        self.assertIn("Rewritten to StorageLevel.MEMORY_AND_DISK, the same level", found[0][2])
        out = rewritten(code, self.rule)
        for expected in ("df.persist(StorageLevel.MEMORY_AND_DISK)", "rdd.persist(pyspark.StorageLevel.MEMORY_ONLY_2)",
                         "x = SL.MEMORY_ONLY\n", "other = Config.MEMORY_AND_DISK_SER"):
            self.assertIn(expected, out)

    def test_renamed_function_keywords(self):
        code = """
            from pyspark.sql.functions import lead
            a = F.lag("k", count=1).over(w)
            b = lead("k", count=2, default=0)
            c = F.regexp_replace(str="v", pattern="[0-9]", replacement="")
            ok = F.lag("k", 1)
            ok2 = F.lag("k", offset=1)
            ok3 = F.count("k")
            ok4 = other.lag("k", count=1)
        """
        found = self._found(code)
        self.assertEqual([f[:2] for f in found], [(3, 1), (4, 1), (5, 1)])
        self.assertIn("Rewritten to offset=", found[0][2])
        out = rewritten(code, self.rule)
        for expected in ('a = F.lag("k", offset=1).over(w)', 'b = lead("k", offset=2, default=0)',
                         'c = F.regexp_replace(string="v", pattern="[0-9]", replacement="")',
                         'ok3 = F.count("k")', 'ok4 = other.lag("k", count=1)'):
            self.assertIn(expected, out)

    def test_star_imported_function_keyword(self):
        module = cst.parse_module("from pyspark.sql.functions import *\nx = lag('k', count=1)\n")
        self.assertEqual([(f["line"], f["tier"]) for f in detect(module, {self.rule})[0]], [(2, 1)])
        self.assertIn("x = lag('k', offset=1)", fix(module, {self.rule})[0])

    def test_removed_mllib_parameters(self):
        code = """
            from pyspark.mllib.clustering import KMeans
            from pyspark.mllib.util import MLUtils
            m1 = KMeans.train(rdd, 2, runs=10)
            m2 = KMeans.train(rdd, 2, 20, 10, "random", seed=7)
            d = MLUtils.loadLibSVMFile(sc, p, multiclass=True)
            d2 = MLUtils.loadLibSVMFile(sc, p, -1, None, True)
            m3 = KMeans.train(rdd, 2, 20, runs=10, seed=7)
            ok = KMeans.train(rdd, 2, 20, initializationMode="random")
            ok2 = KMeans.train(*args)
            ok3 = KMeans.train(rdd, 2, *rest)
        """
        found = self._found(code)
        self.assertEqual([f[:2] for f in found], [(4, 1), (5, 1), (6, 1), (7, 1), (8, 1)])
        self.assertIn("positional argument 4 is runs", found[1][2])
        out = rewritten(code, self.rule)
        for expected in ("m1 = KMeans.train(rdd, 2)", 'm2 = KMeans.train(rdd, 2, 20, "random", seed=7)',
                         "d = MLUtils.loadLibSVMFile(sc, p)", "d2 = MLUtils.loadLibSVMFile(sc, p, -1, None)",
                         "m3 = KMeans.train(rdd, 2, 20, seed=7)", "ok3 = KMeans.train(rdd, 2, *rest)"):
            self.assertIn(expected, out)

    def test_removed_mllib_parameter_with_a_side_effect_stays_manual(self):
        code = """
            from pyspark.mllib.clustering import KMeans
            m = KMeans.train(rdd, 2, 20, next(runs), "random")
            m2 = KMeans.train(rdd, 2, runs=pick())
        """
        found = self._found(code)
        self.assertEqual([f[:2] for f in found], [(3, 3), (4, 3)])
        self.assertIn("side effect", found[0][2])
        out = rewritten(code, self.rule)
        self.assertIn('KMeans.train(rdd, 2, 20, next(runs), "random")', out)
        self.assertIn("KMeans.train(rdd, 2, runs=pick())", out)

    def test_multiline_trailing_argument(self):
        code = """
            from pyspark.mllib.util import MLUtils
            d = MLUtils.loadLibSVMFile(
                sc,
                p,
                multiclass=True,
            )
        """
        self.assertIn("    sc,\n    p,\n)", rewritten(code, self.rule))

    def test_multiclass_metrics_without_label(self):
        code = """
            from pyspark.mllib.evaluation import MulticlassMetrics, MultilabelMetrics
            metrics = MulticlassMetrics(pairs)
            p = metrics.precision()
            r = MulticlassMetrics(pairs).recall()
            f = metrics.fMeasure()
            ok = metrics.precision(1.0)
            ok2 = metrics.accuracy
            labels = MultilabelMetrics(pairs)
            ok3 = labels.precision()
            ok4 = unknown.precision()
            ok5 = metrics.fMeasure(beta=0.5)
        """
        found = self._found(code)
        self.assertEqual([f[:2] for f in found], [(4, 1), (5, 1), (6, 1)])
        self.assertIn("Rewritten to the accuracy property", found[0][2])
        out = rewritten(code, self.rule)
        for expected in ("p = metrics.accuracy\n", "r = MulticlassMetrics(pairs).accuracy\n", "f = metrics.accuracy\n",
                         "ok = metrics.precision(1.0)", "ok3 = labels.precision()", "ok4 = unknown.precision()",
                         "ok5 = metrics.fMeasure(beta=0.5)"):
            self.assertIn(expected, out)

    def test_moved_imports_are_rewritten(self):
        code = """
            from pyspark import PickleSerializer
            from pyspark.sql.types import to_arrow_type
            from pyspark.heapq3 import merge
            from pyspark.sql.types import StructType, from_arrow_schema
            from pyspark import heapq3
            import pyspark.heapq3
            from pyspark.ml.regression import TreeEnsembleModel
            from pyspark.serializers import ArrowStreamPandasSerializer
        """
        found = self._found(code)
        self.assertEqual([f[:2] for f in found], [(2, 1), (3, 1), (4, 1), (5, 3), (6, 3), (7, 3), (8, 3), (9, 3)])
        self.assertIn("pyspark.ml.tree._TreeEnsembleModel", found[6][2])
        out = rewritten(code, self.rule)
        self.assertIn("from pyspark.serializers import PickleSerializer", out)
        self.assertIn("from pyspark.sql.pandas.types import to_arrow_type", out)
        self.assertIn("from heapq import merge", out)
        self.assertIn("from pyspark.sql.types import StructType, from_arrow_schema", out, "a mixed line is reported, not split")


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
        # Not "array<string>": that cast makes the elements nullable, where 2.4's were not.
        self.assertIn("from pyspark.sql.types import ArrayType, StringType, MapType\n", out)
        self.assertIn("F.array().cast(ArrayType(StringType(), False))", out)
        self.assertIn('F.create_map().cast(MapType(StringType(), StringType(), False)).alias("m")', out)
        self.assertIn('ok2 = F.array().cast("array<int>")', out)

    def test_empty_collection_type_names(self):
        rule = "EmptyCollectionTypeDetect"
        joined = rewritten("""
            from pyspark.sql.types import StructType
            a = F.array()
        """, rule)
        self.assertIn("from pyspark.sql.types import StructType, ArrayType, StringType\n", joined)
        star = rewritten("""
            from pyspark.sql.types import *
            a = F.array()
        """, rule)
        self.assertEqual(star.count("import"), 1, "the star import already has them")
        taken = rewritten("""
            StringType = str
            a = F.array()
        """, rule)
        self.assertIn('ArrayType(__import__("pyspark.sql.types").sql.types.StringType(), False)', taken)

    def test_all_rewrites_chain_in_one_file(self):
        code = """
            from pyspark.ml.feature import OneHotEncoderEstimator
            r = Row(b=1, a=2)
            x = spark.read.option("path", p).load("/in")
            e = F.array()
        """
        out = rewritten(code)
        for expected in ("import OneHotEncoder\n", "Row(a=2, b=1)", 'spark.read.load("/in")', "F.array().cast(ArrayType(StringType(), False))"):
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


class StarImportShadowsTest(unittest.TestCase):
    """3.5.5's `from pyspark.sql.functions import *` binds 255 names 2.4.8's did
    not. Probed in docker: the rewritten file prints 2.4.8's output on 3.5.5;
    the original raises (`reduce() missing ... 'merge'`)."""
    rule = "StarImportShadowsFilter"

    @staticmethod
    def run_fix(code: str) -> tuple[str, list[dict]]:
        module = cst.parse_module(textwrap.dedent(code))
        out, _, errors = fix(module, {StarImportShadowsTest.rule})
        assert not errors, errors
        compile(out, "<rewritten>", "exec")
        found, errors = detect(module, {StarImportShadowsTest.rule})
        assert not errors, errors
        return out, found

    def test_builtin_filter_is_rewritten_with_import(self):
        out, found = self.run_fix("""
            \"\"\"Doc.\"\"\"
            from __future__ import annotations
            import os
            from pyspark.sql.functions import *
            def evens(xs):
                return list(filter(lambda x: x % 2 == 0, xs))
            def local(xs, filter):
                return filter(xs)
            keep = map(filter, [])
            df.filter(col("a") > 1)
            spark.range(3).where(col("id") > 0).rdd.filter(bool)
        """)
        self.assertEqual(out.splitlines()[5], "import builtins", "after the opening docstring and imports")
        self.assertIn("return list(builtins.filter(lambda x", out)
        self.assertIn("return filter(xs)", out, "a parameter named filter is not the builtin")
        self.assertIn("keep = map(builtins.filter, [])", out)
        self.assertIn("df.filter(col", out)
        self.assertEqual([(f["line"], f["tier"]) for f in found], [(7, 1), (10, 1)])

    def test_no_star_import_no_change(self):
        out, found = self.run_fix("""
            from pyspark.sql import functions as F
            xs = list(filter(None, [0, 1]))
        """)
        self.assertNotIn("builtins", out)
        self.assertEqual(found, [])

    def test_filter_bound_in_module_is_left(self):
        out, found = self.run_fix("""
            from pyspark.sql.functions import *
            def filter(xs):
                return xs
            ys = filter([1])
        """)
        self.assertNotIn("builtins", out)
        self.assertEqual(found, [])

    def test_names_imported_before_the_star_are_imported_again(self):
        out, found = self.run_fix("""
            import sys
            from functools import reduce
            from math import pi, e as euler
            from typing import cast
            from pyspark.sql.types import StringType
            from numpy import median as median
            from pyspark.sql.functions import *
            from pyspark.sql.functions import col
            total = reduce(lambda a, b: a + b, [1, 2]) * pi
            print(sys.argv, cast(int, 1), StringType(), median([1]), euler)
        """)
        lines = out.splitlines()
        star = lines.index("from pyspark.sql.functions import *")
        self.assertEqual(lines[star + 1:star + 4],
                         ["from functools import reduce", "from math import pi", "from numpy import median as median"])
        self.assertEqual(len(found), 3)
        self.assertTrue(all(f["tier"] == 1 and f["line"] == 8 for f in found))

    def test_an_unused_import_or_a_later_binding_is_left(self):
        out, found = self.run_fix("""
            from functools import reduce
            from pyspark.sql.functions import *
            mode = "fast"
            print(mode)
        """)
        self.assertEqual(found, [])

    def test_a_definition_before_the_star_is_reported(self):
        out, found = self.run_fix("""
            def reduce(xs):
                return xs
            from pyspark.sql.functions import *
            print(reduce([1]))
        """)
        self.assertEqual([(f["line"], f["tier"]) for f in found], [(2, 3)])
        self.assertEqual(out.count("import"), 1)

    def test_builtins_bound_to_something_else_is_reported(self):
        out, found = self.run_fix("""
            from pyspark.sql.functions import *
            builtins = None
            ys = filter(None, [1])
        """)
        self.assertEqual([(f["line"], f["tier"]) for f in found], [(4, 3)])
        self.assertIn("ys = filter(None", out)


if __name__ == "__main__":
    unittest.main()
