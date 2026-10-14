package com.acceldata.openrewrite.spark;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;
import org.openrewrite.test.TypeValidation;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.openrewrite.java.Assertions.java;

// One test per SparkCompileBreakDetect row. Every input here compiles on Spark
// 2.4.8 (the classpath these tests parse against) and not on 3.5.5. A row with
// a SparkCompileBreakRewrite reports only the sites the rewrite leaves.
class SparkCompileBreakDetectTest implements RewriteTest {

    // spark-mllib and spark-streaming come from -DcompileBreakJars (build.sbt),
    // not the shared test classpath.
    @Override
    public void defaults(RecipeSpec spec) {
        List<Path> classpath = new ArrayList<>(JavaParser.runtimeClasspath());
        for (String jar : System.getProperty("compileBreakJars", "").split(File.pathSeparator)) {
            if (!jar.isEmpty()) {
                classpath.add(Paths.get(jar));
            }
        }
        spec.parser(JavaParser.fromJavaVersion().classpath(classpath));
    }

    private static SparkCompileBreakDetect recipe(String sub) {
        return new SparkCompileBreakDetect(sub);
    }

    private static String mark(String sub) {
        return "/*~~(" + recipe(sub).getDescription() + ")~~>*/";
    }

    @Test
    void everyRowHasItsOwnId() {
        List<String> names = SparkCompileBreakDetect.all().stream().map(SparkCompileBreakDetect::getName)
                .collect(Collectors.toList());
        assertThat(names).hasSize(15).doesNotHaveDuplicates()
                .allMatch(n -> n.startsWith("com.acceldata.openrewrite.spark.SparkCompileBreakDetect."));
    }

    @Test
    void sitesTheRewriteHandlesAreNotReported() {
        rewriteRun(spec -> spec.recipes(recipe("MulticlassMetricsNoLabel"), recipe("GbtNumTrees"),
                        recipe("StageInfoAttemptId"), recipe("MlReadWriteContext"), recipe("MlpLayers")),
                java(
                        """
                        import org.apache.spark.ml.classification.GBTClassificationModel;
                        import org.apache.spark.ml.classification.LogisticRegressionModel;
                        import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
                        import org.apache.spark.mllib.evaluation.MulticlassMetrics;
                        import org.apache.spark.scheduler.StageInfo;
                        import org.apache.spark.sql.SQLContext;
                        class A {
                            double a(MulticlassMetrics m) { return m.precision() + m.recall(1.0) + m.accuracy(); }
                            int b(GBTClassificationModel m) { return m.numTrees() + m.getNumTrees(); }
                            int c(StageInfo s) { return s.attemptId() + s.attemptNumber(); }
                            Object d(SQLContext sq) { return LogisticRegressionModel.read().context(sq).load("x"); }
                            int[] e(MultilayerPerceptronClassificationModel m) { return m.layers(); }
                        }
                        """));
    }

    @Test
    void kMeansRuns() {
        String m = mark("KMeansRuns");
        rewriteRun(spec -> spec.recipe(recipe("KMeansRuns")),
                java(
                        """
                        import org.apache.spark.api.java.JavaRDD;
                        import org.apache.spark.mllib.clustering.KMeans;
                        import org.apache.spark.mllib.linalg.Vector;
                        class A {
                            int runs() { return 2; }
                            KMeans a() { return new KMeans().setK(3).setRuns(runs()); }
                            int b(KMeans k) { return k.getRuns(); }
                            Object c(JavaRDD<Vector> d) { return KMeans.train(d.rdd(), 2, 10, runs()); }
                            Object rewritten(JavaRDD<Vector> d) { return KMeans.train(d.rdd(), 2, 10, 3); }
                            Object ok(JavaRDD<Vector> d) { return KMeans.train(d.rdd(), 2, 10, "random"); }
                        }
                        """,
                        """
                        import org.apache.spark.api.java.JavaRDD;
                        import org.apache.spark.mllib.clustering.KMeans;
                        import org.apache.spark.mllib.linalg.Vector;
                        class A {
                            int runs() { return 2; }
                            KMeans a() { return %snew KMeans().setK(3).setRuns(runs()); }
                            int b(KMeans k) { return %sk.getRuns(); }
                            Object c(JavaRDD<Vector> d) { return %sKMeans.train(d.rdd(), 2, 10, runs()); }
                            Object rewritten(JavaRDD<Vector> d) { return KMeans.train(d.rdd(), 2, 10, 3); }
                            Object ok(JavaRDD<Vector> d) { return KMeans.train(d.rdd(), 2, 10, "random"); }
                        }
                        """.formatted(m, m, m)));
    }

    @Test
    void windowBoundaries() {
        rewriteRun(spec -> spec.recipes(recipe("WindowBoundaryFunctions"), recipe("RangeBetweenColumns")),
                java(
                        """
                        import org.apache.spark.sql.expressions.Window;
                        import org.apache.spark.sql.expressions.WindowSpec;
                        import static org.apache.spark.sql.functions.*;
                        class A {
                            WindowSpec a() { return Window.orderBy("a").rangeBetween(lit(-1), lit(1)); }
                            Object b() { return currentRow(); }
                            WindowSpec c() { return Window.orderBy("a").rangeBetween(unboundedPreceding(), lit(0)); }
                            WindowSpec rewritten() { return Window.orderBy("a").rangeBetween(unboundedPreceding(), currentRow()); }
                            WindowSpec ok() { return Window.orderBy("a").rangeBetween(Window.unboundedPreceding(), Window.currentRow()); }
                        }
                        """,
                        """
                        import org.apache.spark.sql.expressions.Window;
                        import org.apache.spark.sql.expressions.WindowSpec;
                        import static org.apache.spark.sql.functions.*;
                        class A {
                            WindowSpec a() { return %sWindow.orderBy("a").rangeBetween(lit(-1), lit(1)); }
                            Object b() { return %scurrentRow(); }
                            WindowSpec c() { return %sWindow.orderBy("a").rangeBetween(%sunboundedPreceding(), lit(0)); }
                            WindowSpec rewritten() { return Window.orderBy("a").rangeBetween(unboundedPreceding(), currentRow()); }
                            WindowSpec ok() { return Window.orderBy("a").rangeBetween(Window.unboundedPreceding(), Window.currentRow()); }
                        }
                        """.formatted(mark("RangeBetweenColumns"), mark("WindowBoundaryFunctions"),
                                mark("RangeBetweenColumns"), mark("WindowBoundaryFunctions"))));
    }

    @Test
    void removedTypesByName() {
        // sun.misc does not exist on this JDK 17 test classpath, as on the CLI's.
        // The encoder call has no byte[] variable and InterfaceStability is used
        // outside an annotation, so their imports stay and are reported.
        rewriteRun(spec -> spec.recipes(recipe("SunMiscBase64"), recipe("InterfaceStability"), recipe("AccumulableApi"))
                        .typeValidationOptions(TypeValidation.none()),
                java(
                        """
                        import sun.misc.BASE64Decoder;
                        import sun.misc.BASE64Encoder;
                        import org.apache.spark.AccumulatorParam;
                        import org.apache.spark.annotation.InterfaceStability;
                        @InterfaceStability.Evolving
                        class A {
                            Object s = InterfaceStability.class;
                            String b(byte[] x) { return new BASE64Encoder().encode(x.clone()); }
                        }
                        """,
                        """
                        import %ssun.misc.BASE64Decoder;
                        import %ssun.misc.BASE64Encoder;
                        import %sorg.apache.spark.AccumulatorParam;
                        import %sorg.apache.spark.annotation.InterfaceStability;
                        @InterfaceStability.Evolving
                        class A {
                            Object s = InterfaceStability.class;
                            String b(byte[] x) { return new BASE64Encoder().encode(x.clone()); }
                        }
                        """.formatted(mark("SunMiscBase64"), mark("SunMiscBase64"), mark("AccumulableApi"),
                                mark("InterfaceStability"))));
    }

    @Test
    void importsOfRewrittenTypesAreNotReported() {
        rewriteRun(spec -> spec.recipes(recipe("SunMiscBase64"), recipe("InterfaceStability"))
                        .typeValidationOptions(TypeValidation.none()),
                java(
                        """
                        import sun.misc.BASE64Encoder;
                        import org.apache.spark.annotation.InterfaceStability;
                        @InterfaceStability.Evolving
                        class A {
                            String b(byte[] x) { return new BASE64Encoder().encode(x); }
                        }
                        """));
    }

    @Test
    void removedMlMethods() {
        rewriteRun(spec -> spec.recipes(recipe("MlKMeansComputeCost"), recipe("ImageSchemaReadImages")),
                java(
                        """
                        import org.apache.spark.ml.clustering.KMeansModel;
                        import org.apache.spark.ml.image.ImageSchema;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        class A {
                            double a(KMeansModel m, Dataset<Row> d) { return m.computeCost(d); }
                            Object c() { return ImageSchema.readImages("x"); }
                        }
                        """,
                        """
                        import org.apache.spark.ml.clustering.KMeansModel;
                        import org.apache.spark.ml.image.ImageSchema;
                        import org.apache.spark.sql.Dataset;
                        import org.apache.spark.sql.Row;
                        class A {
                            double a(KMeansModel m, Dataset<Row> d) { return %sm.computeCost(d); }
                            Object c() { return %sImageSchema.readImages("x"); }
                        }
                        """.formatted(mark("MlKMeansComputeCost"), mark("ImageSchemaReadImages"))));
    }

    @Test
    void unionWithList() {
        // A type variable cannot be written as the array's type; String can.
        rewriteRun(spec -> spec.recipe(recipe("UnionWithList")),
                java(
                        """
                        import java.util.List;
                        import org.apache.spark.api.java.JavaRDD;
                        import org.apache.spark.api.java.JavaSparkContext;
                        class A {
                            <T> JavaRDD<T> a(JavaSparkContext jsc, JavaRDD<T> f, List<JavaRDD<T>> rest) { return jsc.union(f, rest); }
                            JavaRDD<String> rewritten(JavaSparkContext jsc, JavaRDD<String> f, List<JavaRDD<String>> rest) { return jsc.union(f, rest); }
                        }
                        """,
                        """
                        import java.util.List;
                        import org.apache.spark.api.java.JavaRDD;
                        import org.apache.spark.api.java.JavaSparkContext;
                        class A {
                            <T> JavaRDD<T> a(JavaSparkContext jsc, JavaRDD<T> f, List<JavaRDD<T>> rest) { return %sjsc.union(f, rest); }
                            JavaRDD<String> rewritten(JavaSparkContext jsc, JavaRDD<String> f, List<JavaRDD<String>> rest) { return jsc.union(f, rest); }
                        }
                        """.formatted(mark("UnionWithList"))));
    }

    @Test
    void flatMapValuesIterable() {
        rewriteRun(spec -> spec.recipe(recipe("FlatMapValuesIterable")),
                java(
                        """
                        import java.util.Arrays;
                        import java.util.Collections;
                        import java.util.List;
                        import org.apache.spark.api.java.JavaPairRDD;
                        class A {
                            static List<String> split(String v) { return Arrays.asList(v.split(",")); }
                            JavaPairRDD<String, String> a(JavaPairRDD<String, String> p) { return p.flatMapValues(A::split); }
                            JavaPairRDD<String, String> b(JavaPairRDD<String, String> p) { return p.flatMapValues(v -> Collections.emptyList()); }
                            JavaPairRDD<String, String> rewritten(JavaPairRDD<String, String> p) { return p.flatMapValues(v -> split(v)); }
                        }
                        """,
                        """
                        import java.util.Arrays;
                        import java.util.Collections;
                        import java.util.List;
                        import org.apache.spark.api.java.JavaPairRDD;
                        class A {
                            static List<String> split(String v) { return Arrays.asList(v.split(",")); }
                            JavaPairRDD<String, String> a(JavaPairRDD<String, String> p) { return %sp.flatMapValues(A::split); }
                            JavaPairRDD<String, String> b(JavaPairRDD<String, String> p) { return %sp.flatMapValues(v -> Collections.emptyList()); }
                            JavaPairRDD<String, String> rewritten(JavaPairRDD<String, String> p) { return p.flatMapValues(v -> split(v)); }
                        }
                        """.formatted(mark("FlatMapValuesIterable"), mark("FlatMapValuesIterable"))));
    }
}
