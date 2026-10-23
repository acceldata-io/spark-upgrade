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

// One test per SparkCompileBreakRewrite row: inputs compile on Spark 2.4.8, the
// outputs on 3.5.5. The shapes each row leaves are SparkCompileBreakDetectTest's.
class SparkCompileBreakRewriteTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        List<Path> classpath = new ArrayList<>(JavaParser.runtimeClasspath());
        for (String jar : System.getProperty("compileBreakJars", "").split(File.pathSeparator)) {
            if (!jar.isEmpty()) {
                classpath.add(Paths.get(jar));
            }
        }
        // The templates name 3.5.5-only and JDK-only members; type them loosely.
        spec.parser(JavaParser.fromJavaVersion().classpath(classpath)).typeValidationOptions(TypeValidation.none());
    }

    private static SparkCompileBreakRewrite recipe(String sub) {
        return new SparkCompileBreakRewrite(sub);
    }

    @Test
    void everyRowHasItsOwnId() {
        List<String> names = SparkCompileBreakRewrite.all().stream().map(SparkCompileBreakRewrite::getName)
                .collect(Collectors.toList());
        assertThat(names).hasSize(11).doesNotHaveDuplicates()
                .allMatch(n -> n.startsWith("com.acceldata.openrewrite.spark.SparkCompileBreakRewrite."));
    }

    @Test
    void renamedAccessors() {
        rewriteRun(spec -> spec.recipes(recipe("MulticlassMetricsNoLabel"), recipe("GbtNumTrees"),
                        recipe("StageInfoAttemptId"), recipe("MlpLayers")),
                java(
                        """
                        import org.apache.spark.ml.classification.GBTClassificationModel;
                        import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
                        import org.apache.spark.ml.regression.GBTRegressionModel;
                        import org.apache.spark.mllib.evaluation.MulticlassMetrics;
                        import org.apache.spark.scheduler.StageInfo;
                        class A {
                            double a(MulticlassMetrics m) { return m.precision() + m.recall() + m.fMeasure() + m.precision(1.0); }
                            int b(GBTClassificationModel m, GBTRegressionModel r) { return m.numTrees() + r.numTrees(); }
                            int c(StageInfo s) { return s.attemptId(); }
                            int[] d(MultilayerPerceptronClassificationModel m) { return m.layers(); }
                        }
                        """,
                        """
                        import org.apache.spark.ml.classification.GBTClassificationModel;
                        import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
                        import org.apache.spark.ml.regression.GBTRegressionModel;
                        import org.apache.spark.mllib.evaluation.MulticlassMetrics;
                        import org.apache.spark.scheduler.StageInfo;
                        class A {
                            double a(MulticlassMetrics m) { return m.accuracy() + m.accuracy() + m.accuracy() + m.precision(1.0); }
                            int b(GBTClassificationModel m, GBTRegressionModel r) { return m.getNumTrees() + r.getNumTrees(); }
                            int c(StageInfo s) { return s.attemptNumber(); }
                            int[] d(MultilayerPerceptronClassificationModel m) { return m.getLayers(); }
                        }
                        """));
    }

    @Test
    void kMeansRunsDropped() {
        rewriteRun(spec -> spec.recipe(recipe("KMeansRuns")),
                java(
                        """
                        import org.apache.spark.api.java.JavaRDD;
                        import org.apache.spark.mllib.clustering.KMeans;
                        import org.apache.spark.mllib.linalg.Vector;
                        class A {
                            KMeans a() {
                                return new KMeans()
                                        .setK(3)
                                        .setRuns(2)
                                        .setSeed(7L);
                            }
                            Object b(JavaRDD<Vector> d, int runs) { return KMeans.train(d.rdd(), 2, 10, runs); }
                            Object c(JavaRDD<Vector> d) { return KMeans.train(d.rdd(), 2, 10, 3, "random", 7L); }
                        }
                        """,
                        """
                        import org.apache.spark.api.java.JavaRDD;
                        import org.apache.spark.mllib.clustering.KMeans;
                        import org.apache.spark.mllib.linalg.Vector;
                        class A {
                            KMeans a() {
                                return new KMeans()
                                        .setK(3)
                                        .setSeed(7L);
                            }
                            Object b(JavaRDD<Vector> d, int runs) { return KMeans.train(d.rdd(), 2, 10); }
                            Object c(JavaRDD<Vector> d) { return KMeans.train(d.rdd(), 2, 10, "random", 7L); }
                        }
                        """));
    }

    @Test
    void mlContextBecomesSession() {
        rewriteRun(spec -> spec.recipe(recipe("MlReadWriteContext")),
                java(
                        """
                        import org.apache.spark.ml.classification.LogisticRegressionModel;
                        import org.apache.spark.sql.SQLContext;
                        class A {
                            void a(LogisticRegressionModel m, SQLContext sq) throws Exception { m.write().context(sq).save("x"); }
                            Object b(SQLContext sq) { return LogisticRegressionModel.read().context(sq).load("x"); }
                        }
                        """,
                        """
                        import org.apache.spark.ml.classification.LogisticRegressionModel;
                        import org.apache.spark.sql.SQLContext;
                        class A {
                            void a(LogisticRegressionModel m, SQLContext sq) throws Exception { m.write().session(sq.sparkSession()).save("x"); }
                            Object b(SQLContext sq) { return LogisticRegressionModel.read().session(sq.sparkSession()).load("x"); }
                        }
                        """));
    }

    @Test
    void windowBoundariesBecomeLongs() {
        rewriteRun(spec -> spec.recipe(recipe("WindowBoundaryFunctions")),
                java(
                        """
                        import org.apache.spark.sql.expressions.WindowSpec;
                        import org.apache.spark.sql.functions;
                        import static org.apache.spark.sql.functions.currentRow;
                        import static org.apache.spark.sql.functions.unboundedPreceding;
                        class A {
                            WindowSpec a(WindowSpec w) { return w.rangeBetween(unboundedPreceding(), currentRow()); }
                            WindowSpec b(WindowSpec w) { return w.rangeBetween(functions.currentRow(), functions.unboundedFollowing()); }
                        }
                        """,
                        """
                        import org.apache.spark.sql.expressions.Window;
                        import org.apache.spark.sql.expressions.WindowSpec;
                        import org.apache.spark.sql.functions;

                        class A {
                            WindowSpec a(WindowSpec w) { return w.rangeBetween(Window.unboundedPreceding(), Window.currentRow()); }
                            WindowSpec b(WindowSpec w) { return w.rangeBetween(Window.currentRow(), Window.unboundedFollowing()); }
                        }
                        """));
    }

    @Test
    void interfaceStabilityAnnotations() {
        rewriteRun(spec -> spec.recipe(recipe("InterfaceStability")),
                java(
                        """
                        import org.apache.spark.annotation.InterfaceStability;
                        @InterfaceStability.Evolving
                        class A {
                            @InterfaceStability.Unstable
                            void a() { }
                        }
                        """,
                        """
                        import org.apache.spark.annotation.Evolving;
                        import org.apache.spark.annotation.Unstable;

                        @Evolving
                        class A {
                            @Unstable
                            void a() { }
                        }
                        """));
    }

    @Test
    void base64EncoderBecomesMimeEncoder() {
        rewriteRun(spec -> spec.recipe(recipe("SunMiscBase64")),
                java(
                        """
                        import sun.misc.BASE64Encoder;
                        class A {
                            String a(byte[] b) {
                                String s = new BASE64Encoder().encode(b);
                                return s + new BASE64Encoder().encodeBuffer(b).trim();
                            }
                        }
                        """,
                        """
                        import java.util.Base64;

                        class A {
                            String a(byte[] b) {
                                String s = Base64.getMimeEncoder(76, System.lineSeparator().getBytes()).encodeToString(b) + (b.length > 0 && b.length % 57 == 0 ? System.lineSeparator() : "");
                                return s + (Base64.getMimeEncoder(76, System.lineSeparator().getBytes()).encodeToString(b) + (b.length > 0 ? System.lineSeparator() : "")).trim();
                            }
                        }
                        """));
    }

    @Test
    void base64OfAnExpressionGoesThroughOneHelper() {
        // The rewrite reads its argument three times: an expression is passed to a helper on the
        // top-level class instead, evaluated once.
        rewriteRun(spec -> spec.recipe(recipe("SunMiscBase64")),
                java(
                        """
                        import java.nio.charset.StandardCharsets;
                        class A {
                            static String a(String id) {
                                return new sun.misc.BASE64Encoder().encode(id.getBytes(StandardCharsets.UTF_8));
                            }
                            static class B {
                                String b(byte[][] rows) { return new sun.misc.BASE64Encoder().encodeBuffer(rows[0]).trim(); }
                            }
                        }
                        """,
                        """
                        import java.nio.charset.StandardCharsets;
                        import java.util.Base64;

                        class A {
                            static String a(String id) {
                                return sunBase64Encode(id.getBytes(StandardCharsets.UTF_8));
                            }
                            static class B {
                                String b(byte[][] rows) { return sunBase64EncodeBuffer(rows[0]).trim(); }
                            }

                            private static String sunBase64Encode(byte[] bytes) {
                                return Base64.getMimeEncoder(76, System.lineSeparator().getBytes()).encodeToString(bytes) + (bytes.length > 0 && bytes.length % 57 == 0 ? System.lineSeparator() : "");
                            }

                            private static String sunBase64EncodeBuffer(byte[] bytes) {
                                return Base64.getMimeEncoder(76, System.lineSeparator().getBytes()).encodeToString(bytes) + (bytes.length > 0 ? System.lineSeparator() : "");
                            }
                        }
                        """));
    }

    @Test
    void base64LeavesTheDecoder() {
        // The decoder stays (see the class comment): so does its import.
        rewriteRun(spec -> spec.recipe(recipe("SunMiscBase64")),
                java(
                        """
                        import sun.misc.BASE64Decoder;
                        class A {
                            byte[] c(String s) throws Exception { return new BASE64Decoder().decodeBuffer(s); }
                        }
                        """));
    }

    @Test
    void unionOfOneArray() {
        rewriteRun(spec -> spec.recipe(recipe("UnionWithList")),
                java(
                        """
                        import java.util.List;
                        import org.apache.spark.api.java.JavaPairRDD;
                        import org.apache.spark.api.java.JavaRDD;
                        import org.apache.spark.api.java.JavaSparkContext;
                        import org.apache.spark.streaming.api.java.JavaDStream;
                        import org.apache.spark.streaming.api.java.JavaStreamingContext;
                        class A {
                            JavaRDD<String> a(JavaSparkContext jsc, JavaRDD<String> f, List<JavaRDD<String>> rest) {
                                return jsc.union(f, rest);
                            }
                            long b(JavaSparkContext jsc, JavaPairRDD<String, Integer> f, List<JavaPairRDD<String, Integer>> rest) {
                                return jsc.union(f, rest).count();
                            }
                            JavaDStream<String> c(JavaStreamingContext ssc, JavaDStream<String> f, List<JavaDStream<String>> rest) {
                                return ssc.union(f, rest);
                            }
                        }
                        """,
                        """
                        import java.util.List;
                        import java.util.stream.Stream;

                        import org.apache.spark.api.java.JavaPairRDD;
                        import org.apache.spark.api.java.JavaRDD;
                        import org.apache.spark.api.java.JavaSparkContext;
                        import org.apache.spark.streaming.api.java.JavaDStream;
                        import org.apache.spark.streaming.api.java.JavaStreamingContext;

                        class A {
                            JavaRDD<String> a(JavaSparkContext jsc, JavaRDD<String> f, List<JavaRDD<String>> rest) {
                                return jsc.union((JavaRDD<String>[]) Stream.concat(Stream.of(f), rest.stream()).toArray(JavaRDD[]::new));
                            }
                            long b(JavaSparkContext jsc, JavaPairRDD<String, Integer> f, List<JavaPairRDD<String, Integer>> rest) {
                                return jsc.union((JavaPairRDD<String, Integer>[]) Stream.concat(Stream.of(f), rest.stream()).toArray(JavaPairRDD[]::new)).count();
                            }
                            JavaDStream<String> c(JavaStreamingContext ssc, JavaDStream<String> f, List<JavaDStream<String>> rest) {
                                return ssc.union((JavaDStream<String>[]) Stream.concat(Stream.of(f), rest.stream()).toArray(JavaDStream[]::new));
                            }
                        }
                        """));
    }

    @Test
    void flatMapValuesReturnsAnIterator() {
        rewriteRun(spec -> spec.recipe(recipe("FlatMapValuesIterable")),
                java(
                        """
                        import java.util.Arrays;
                        import java.util.Collections;
                        import java.util.List;
                        import org.apache.spark.api.java.JavaPairRDD;
                        class A {
                            JavaPairRDD<String, String> a(JavaPairRDD<String, String> p) {
                                return p.flatMapValues(v -> Arrays.asList(v.split(",")));
                            }
                            JavaPairRDD<String, String> b(JavaPairRDD<String, String> p) {
                                return p.flatMapValues(v -> {
                                    if (v == null) {
                                        return Collections.<String>emptyList();
                                    }
                                    List<String> parts = Arrays.asList(v.split(";"));
                                    return parts;
                                });
                            }
                            JavaPairRDD<String, String> c(JavaPairRDD<String, String> p, List<String> none) {
                                return p.flatMapValues(v -> v.isEmpty() ? none : Arrays.asList(v));
                            }
                        }
                        """,
                        """
                        import java.util.Arrays;
                        import java.util.Collections;
                        import java.util.List;
                        import org.apache.spark.api.java.JavaPairRDD;
                        class A {
                            JavaPairRDD<String, String> a(JavaPairRDD<String, String> p) {
                                return p.flatMapValues(v -> Arrays.asList(v.split(",")).iterator());
                            }
                            JavaPairRDD<String, String> b(JavaPairRDD<String, String> p) {
                                return p.flatMapValues(v -> {
                                    if (v == null) {
                                        return Collections.<String>emptyList().iterator();
                                    }
                                    List<String> parts = Arrays.asList(v.split(";"));
                                    return parts.iterator();
                                });
                            }
                            JavaPairRDD<String, String> c(JavaPairRDD<String, String> p, List<String> none) {
                                return p.flatMapValues(v -> (v.isEmpty() ? none : Arrays.asList(v)).iterator());
                            }
                        }
                        """));
    }
}
