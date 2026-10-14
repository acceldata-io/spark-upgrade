package com.acceldata.openrewrite.spark;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Java source that compiles on Spark 2.4.8 / JDK 8 and does not on Spark
 * 3.5.5 / JDK 17. The Java twin of the Scalafix {@code CompileBreakDetect}:
 * one table, and one recipe instance per row, so each break has its own id
 * ({@code SparkCompileBreakDetect.<Sub>}) and its own registry text on the CLI
 * side. Detect only (Tier 3): since 2026-10-14 it reports only the sites
 * {@link SparkCompileBreakRewrite} leaves as written.
 *
 * <p>Every row was probed 2026-10-13: a Maven project compiles each case on
 * 2.4.8 + JDK 8 and javac rejects it on 3.5.5 + JDK 17; the removed members
 * were checked with javap on both jars. Not included: SparkHadoopUtil (still
 * reachable from Java; its {@code private[spark]} is Scala-only) and
 * databricks Avro ({@code format("com.databricks.spark.avro")} still works on
 * 3.5.5). The SGD classes are SparkMllibSgdModelUsageDetect's.
 *
 * <p>Methods are matched by resolved type, so they need the 2.4.8 classpath
 * the CLI passes. Types removed outright (InterfaceStability, sun.misc,
 * AccumulatorParam) are matched by their fully-qualified name as written,
 * which is the import in practice, so they are found on any classpath.
 */
public class SparkCompileBreakDetect extends Recipe {

    private static final class Break {
        final String sub;
        final String message;
        final List<MethodMatcher> methods = new ArrayList<>();
        final List<String> typeNames = new ArrayList<>();

        Break(String sub, String message) {
            this.sub = sub;
            this.message = message;
        }

        Break methods(String... patterns) {
            for (String p : patterns) {
                methods.add(new MethodMatcher(p, true));
            }
            return this;
        }

        Break types(String... names) {
            typeNames.addAll(Arrays.asList(names));
            return this;
        }
    }

    private static final Map<String, Break> BREAKS = new LinkedHashMap<>();

    private static void add(Break b) {
        BREAKS.put(b.sub, b);
    }

    static {
        add(new Break("MulticlassMetricsNoLabel",
                "MulticlassMetrics.precision(), recall() and fMeasure() without a label were removed in Spark 3.0; in " +
                        "2.4 each returned the overall accuracy. Replace the call with accuracy().")
                .methods("org.apache.spark.mllib.evaluation.MulticlassMetrics precision()",
                        "org.apache.spark.mllib.evaluation.MulticlassMetrics recall()",
                        "org.apache.spark.mllib.evaluation.MulticlassMetrics fMeasure()"));
        add(new Break("KMeansRuns",
                "The mllib KMeans runs parameter was removed in Spark 3.0 (setRuns, getRuns and the train overloads " +
                        "taking runs); it had no effect since 2.0. Delete .setRuns(n), and remove the runs argument " +
                        "from KMeans.train(data, k, maxIterations, runs, ...).")
                .methods("org.apache.spark.mllib.clustering.KMeans setRuns(int)",
                        "org.apache.spark.mllib.clustering.KMeans getRuns()",
                        "org.apache.spark.mllib.clustering.KMeans train(org.apache.spark.rdd.RDD, int, int, int, ..)"));
        add(new Break("GbtNumTrees",
                "GBTClassificationModel.numTrees() and GBTRegressionModel.numTrees() were removed in Spark 3.0. Use " +
                        "getNumTrees(), which returns the same number.")
                .methods("org.apache.spark.ml.classification.GBTClassificationModel numTrees()",
                        "org.apache.spark.ml.regression.GBTRegressionModel numTrees()"));
        add(new Break("StageInfoAttemptId",
                "StageInfo.attemptId() is private from Spark 3.0. Use attemptNumber(), which returns the same value.")
                .methods("org.apache.spark.scheduler.StageInfo attemptId()"));
        add(new Break("InterfaceStability",
                "org.apache.spark.annotation.InterfaceStability was removed in Spark 3.0. Use " +
                        "org.apache.spark.annotation.Stable / Evolving / Unstable instead of InterfaceStability.Stable / " +
                        "Evolving / Unstable.")
                .types("org.apache.spark.annotation.InterfaceStability"));
        add(new Break("MlReadWriteContext",
                "MLWriter.context(sqlContext) and MLReader.context(sqlContext) were removed in Spark 3.0. Use " +
                        ".session(sqlContext.sparkSession()), or .session(spark) with your SparkSession.")
                .methods("org.apache.spark.ml.util.BaseReadWrite context(org.apache.spark.sql.SQLContext)"));
        add(new Break("WindowBoundaryFunctions",
                "functions.currentRow(), unboundedPreceding() and unboundedFollowing() were removed in Spark 3.0, as " +
                        "was the rangeBetween(Column, Column) overload they were made for. Use the long boundaries " +
                        "with rangeBetween(long, long) or rowsBetween: Window.currentRow(), " +
                        "Window.unboundedPreceding(), Window.unboundedFollowing().")
                .methods("org.apache.spark.sql.functions currentRow()",
                        "org.apache.spark.sql.functions unboundedPreceding()",
                        "org.apache.spark.sql.functions unboundedFollowing()"));
        add(new Break("RangeBetweenColumns",
                "rangeBetween(Column, Column) was removed in Spark 3.0; only rangeBetween(long, long) remains. Pass " +
                        "long boundaries: Window.unboundedPreceding() / Window.currentRow() / " +
                        "Window.unboundedFollowing() or a literal offset.")
                .methods("org.apache.spark.sql.expressions.Window rangeBetween(org.apache.spark.sql.Column, org.apache.spark.sql.Column)",
                        "org.apache.spark.sql.expressions.WindowSpec rangeBetween(org.apache.spark.sql.Column, org.apache.spark.sql.Column)"));
        add(new Break("SunMiscBase64",
                "sun.misc.BASE64Encoder and BASE64Decoder were removed in JDK 9; Spark 3.5.5 runs on JDK 17. " +
                        "Decoding: java.util.Base64.getMimeDecoder().decode(s) gives the same bytes for well-formed " +
                        "input (sun's decoder returned other bytes for unpadded or malformed input). Encoding (the " +
                        "tool rewrites new BASE64Encoder().encode(b) on a byte[] variable): " +
                        "Base64.getMimeEncoder(76, System.lineSeparator().getBytes()).encodeToString(b) gives sun's " +
                        "lines; encode also ended with a line separator when b's length was a non-zero multiple of " +
                        "57, encodeBuffer after any non-empty input.")
                .types("sun.misc.BASE64Encoder", "sun.misc.BASE64Decoder"));
        add(new Break("MlKMeansComputeCost",
                "ml KMeansModel.computeCost(dataset) was removed in Spark 3.0. For the training data's cost use " +
                        "model.summary().trainingCost(). For other data, ClusteringEvaluator gives the silhouette (a " +
                        "different metric); the same cost needs the mllib KMeansModel's computeCost(rdd).")
                .methods("org.apache.spark.ml.clustering.KMeansModel computeCost(..)"));
        add(new Break("ImageSchemaReadImages",
                "ImageSchema.readImages was removed in Spark 3.0. Use the image data source: " +
                        "spark.read().format(\"image\").load(path) (option dropInvalid for dropImageFailures, " +
                        "recursiveFileLookup for recursive).")
                .methods("org.apache.spark.ml.image.ImageSchema readImages(..)"));
        add(new Break("MlpLayers",
                "MultilayerPerceptronClassificationModel.layers() returns an IntArrayParam from Spark 3.0, not an " +
                        "int[]. Use getLayers(), the int[] layers() used to return.")
                .methods("org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel layers()"));
        add(new Break("AccumulableApi",
                "AccumulatorParam and AccumulableParam (the v1 accumulator API) were removed in Spark 3.0. Extend " +
                        "org.apache.spark.util.AccumulatorV2 and register it with sc.register(acc, name); for a " +
                        "collection use sc.collectionAccumulator(name).")
                .types("org.apache.spark.AccumulatorParam", "org.apache.spark.AccumulableParam")
                .methods("org.apache.spark.api.java.JavaSparkContext accumulable(..)"));
        add(new Break("UnionWithList",
                "JavaSparkContext.union(first, List rest) and JavaStreamingContext.union(first, List rest) were " +
                        "removed in Spark 3.0; only union(T... rdds) remains. Pass one array: build a List with first " +
                        "and rest and call union(all.toArray(new JavaRDD[0])), or union(first, a, b) for a fixed set.")
                .methods("org.apache.spark.api.java.JavaSparkContext union(.., java.util.List)",
                        "org.apache.spark.streaming.api.java.JavaStreamingContext union(.., java.util.List)"));
        add(new Break("FlatMapValuesIterable",
                "JavaPairRDD.flatMapValues takes a FlatMapFunction returning an Iterator from Spark 3.0 (2.4: a " +
                        "Function returning an Iterable), so a function returning a List or other Iterable no longer " +
                        "compiles. Return an Iterator: append .iterator() to the returned value, e.g. " +
                        "v -> Arrays.asList(v.split(\",\")).iterator().")
                .methods("org.apache.spark.api.java.JavaPairRDD flatMapValues(..)"));
    }

    /** One recipe per row, in table order, for RecipeCatalog. */
    public static List<SparkCompileBreakDetect> all() {
        List<SparkCompileBreakDetect> recipes = new ArrayList<>();
        for (String sub : BREAKS.keySet()) {
            recipes.add(new SparkCompileBreakDetect(sub));
        }
        return Collections.unmodifiableList(recipes);
    }

    private final String sub;

    // An unknown sub (OpenRewrite's introspection constructs recipes with a
    // null argument) gives a recipe that matches nothing.
    @JsonCreator
    public SparkCompileBreakDetect(@JsonProperty("sub") String sub) {
        this.sub = sub;
    }

    public String getSub() {
        return sub;
    }

    /** The id the Runner reports and JavaRuleRegistry keys on. */
    @Override
    public String getName() {
        return SparkCompileBreakDetect.class.getName() + "." + sub;
    }

    @Override
    public String getDisplayName() {
        return "Detect Spark 3.5.5 / JDK 17 compile break: " + sub;
    }

    @Override
    public String getDescription() {
        Break b = BREAKS.get(sub);
        return b == null ? "Detects source that compiles on Spark 2.4.8 / JDK 8 and not on Spark 3.5.5 / JDK 17." : b.message;
    }

    // `a.b.C` written as a qualified name: an import, or a fully-qualified use.
    private static String qualifiedName(Expression e) {
        if (e instanceof J.Identifier) {
            return ((J.Identifier) e).getSimpleName();
        }
        if (e instanceof J.FieldAccess) {
            J.FieldAccess fa = (J.FieldAccess) e;
            String target = qualifiedName(fa.getTarget());
            return target == null ? null : target + "." + fa.getSimpleName();
        }
        return null;
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        Break b = BREAKS.get(sub);
        if (b == null) {
            return TreeVisitor.noop();
        }
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                // The sites SparkCompileBreakRewrite rewrites are not reported.
                boolean rewritten = SparkCompileBreakRewrite.handles(
                        "RangeBetweenColumns".equals(sub) ? "WindowBoundaryFunctions" : sub, method) ||
                        ("WindowBoundaryFunctions".equals(sub) && SparkCompileBreakRewrite.handledBoundary(method, getCursor()));
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                for (MethodMatcher matcher : b.methods) {
                    if (matcher.matches(method) && !rewritten) {
                        return SearchResult.found(m, b.message);
                    }
                }
                return m;
            }

            @Override
            public J.FieldAccess visitFieldAccess(J.FieldAccess fieldAccess, ExecutionContext ctx) {
                String name = qualifiedName(fieldAccess);
                boolean rewritten = ("sun.misc.BASE64Encoder".equals(name) ||
                        "org.apache.spark.annotation.InterfaceStability".equals(name)) &&
                        SparkCompileBreakRewrite.importGoes(sub, getCursor().firstEnclosingOrThrow(J.CompilationUnit.class));
                if (b.typeNames.contains(name) && !rewritten) {
                    return SearchResult.found(fieldAccess, b.message);
                }
                return super.visitFieldAccess(fieldAccess, ctx);
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SparkCompileBreakDetect && Objects.equals(((SparkCompileBreakDetect) o).sub, sub);
    }

    @Override
    public int hashCode() {
        return Objects.hash(SparkCompileBreakDetect.class, sub);
    }
}
