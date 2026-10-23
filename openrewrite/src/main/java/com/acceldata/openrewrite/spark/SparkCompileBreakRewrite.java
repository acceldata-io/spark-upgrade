package com.acceldata.openrewrite.spark;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.openrewrite.Cursor;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.Tree;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JContainer;
import org.openrewrite.java.tree.JRightPadded;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.Space;
import org.openrewrite.java.tree.Statement;
import org.openrewrite.marker.Markers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The exact Spark 3.5.5 / JDK 17 form of the {@link SparkCompileBreakDetect}
 * rows that have one, one recipe instance per row ({@code
 * SparkCompileBreakRewrite.<Sub>}, Tier 1). Each site it does not handle is
 * left as written and reported by the detect row of the same name; {@link
 * #handles} is the shared test.
 *
 * <p>Probed (2.4.8 sources and javap; the old call on 2.4.8 / JDK 8
 * against the new one on 3.5.5 / JDK 17):
 * <ul>
 * <li>MulticlassMetrics precision()/recall()/fMeasure() -> accuracy(): 2.4.8
 *     defines them as {@code lazy val precision: Double = accuracy}.
 * <li>KMeans: 2.4.8's setRuns only logs and returns this, and train(data, k,
 *     maxIterations, runs, ...) builds the same KMeans as train without runs.
 *     Only a literal or a variable is dropped (no side effect is lost).
 * <li>GBT numTrees() and getNumTrees() are both trees.length; StageInfo's
 *     attemptNumber() returns attemptId; MLWriter/MLReader context(sqlContext)
 *     is session(sqlContext.sparkSession()); an MLP model's getLayers() is the
 *     array 2.4.8's layers() returned.
 * <li>rangeBetween(functions.unboundedPreceding(), functions.currentRow())
 *     and the like -> rangeBetween(Window.unboundedPreceding(),
 *     Window.currentRow()): the same frame, rows and column name. A lit(n)
 *     bound is not rewritten (lit(0) names the frame "0 FOLLOWING", 0L
 *     "CURRENT ROW"; a long offset fails on a date order column).
 * <li>@InterfaceStability.X -> @X from org.apache.spark.annotation.
 * <li>new BASE64Encoder().encode(b) / encodeBuffer(b), b a byte[]:
 *     MIME lines of 76 characters joined by the line separator, which is what
 *     sun's encoder printed (println); encode also ended with a separator
 *     when b's length was a non-zero multiple of 57, encodeBuffer after any
 *     non-empty input. Byte-for-byte equal for lengths 0-200 (JDK 8 against
 *     17). The decoder is not rewritten: getMimeDecoder() decodes well-formed
 *     input the same, but sun's decoder returned other bytes for unpadded or
 *     malformed input (QUJDRA, " QUJD ", QUJD!!RA==) where Base64 throws or
 *     skips. The rewrite reads b three times, so a variable is rewritten in
 *     place and any other expression (s.getBytes(UTF_8)) is passed to one
 *     private static helper the rewrite adds to the file's top-level class,
 *     which evaluates it once.
 * <li>JavaSparkContext/JavaStreamingContext union(first, rest) -> union of
 *     one array, first then rest in order: 2.4.8 built
 *     sc.union(first +: rest), 3.5.5's union(rdds...) is sc.union(rdds).
 * <li>flatMapValues(v -> iterable) -> v -> iterable.iterator(), in an
 *     expression lambda or on every return of a block lambda: 2.4.8 iterated
 *     the Iterable once, in order.
 * </ul>
 * sun.misc does not exist on the JDK 17 the parser runs on, so the encoder is
 * matched by its name as written.
 */
public class SparkCompileBreakRewrite extends Recipe {

    private static final String WINDOW = "org.apache.spark.sql.expressions.Window";

    private static final Map<String, String[]> RENAMES = new LinkedHashMap<>();

    static {
        RENAMES.put("MulticlassMetricsNoLabel", new String[]{"accuracy",
                "org.apache.spark.mllib.evaluation.MulticlassMetrics precision()",
                "org.apache.spark.mllib.evaluation.MulticlassMetrics recall()",
                "org.apache.spark.mllib.evaluation.MulticlassMetrics fMeasure()"});
        RENAMES.put("GbtNumTrees", new String[]{"getNumTrees",
                "org.apache.spark.ml.classification.GBTClassificationModel numTrees()",
                "org.apache.spark.ml.regression.GBTRegressionModel numTrees()"});
        RENAMES.put("StageInfoAttemptId", new String[]{"attemptNumber",
                "org.apache.spark.scheduler.StageInfo attemptId()"});
        RENAMES.put("MlpLayers", new String[]{"getLayers",
                "org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel layers()"});
    }

    private static final List<String> SUBS = Arrays.asList("MulticlassMetricsNoLabel", "KMeansRuns", "GbtNumTrees",
            "StageInfoAttemptId", "InterfaceStability", "MlReadWriteContext", "WindowBoundaryFunctions",
            "SunMiscBase64", "MlpLayers", "UnionWithList", "FlatMapValuesIterable");

    private static final MethodMatcher SET_RUNS = new MethodMatcher("org.apache.spark.mllib.clustering.KMeans setRuns(int)");
    private static final MethodMatcher TRAIN_RUNS =
            new MethodMatcher("org.apache.spark.mllib.clustering.KMeans train(org.apache.spark.rdd.RDD, int, int, int, ..)");
    private static final MethodMatcher CONTEXT =
            new MethodMatcher("org.apache.spark.ml.util.BaseReadWrite context(org.apache.spark.sql.SQLContext)", true);
    private static final MethodMatcher RANGE_BETWEEN = new MethodMatcher(
            "org.apache.spark.sql.expressions.* rangeBetween(org.apache.spark.sql.Column, org.apache.spark.sql.Column)");
    private static final MethodMatcher BOUNDARY = new MethodMatcher("org.apache.spark.sql.functions *()");
    private static final Set<String> BOUNDARIES =
            new LinkedHashSet<>(Arrays.asList("unboundedPreceding", "currentRow", "unboundedFollowing"));
    private static final MethodMatcher UNION_RDD = new MethodMatcher("org.apache.spark.api.java.JavaSparkContext union(.., java.util.List)");
    private static final MethodMatcher UNION_STREAM =
            new MethodMatcher("org.apache.spark.streaming.api.java.JavaStreamingContext union(.., java.util.List)");
    private static final MethodMatcher FLAT_MAP_VALUES = new MethodMatcher("org.apache.spark.api.java.JavaPairRDD flatMapValues(..)");
    private static final Set<String> ANNOTATIONS = new LinkedHashSet<>(Arrays.asList("Stable", "Evolving", "Unstable"));

    /** One recipe per row, for RecipeCatalog. */
    public static List<SparkCompileBreakRewrite> all() {
        List<SparkCompileBreakRewrite> recipes = new ArrayList<>();
        for (String sub : SUBS) {
            recipes.add(new SparkCompileBreakRewrite(sub));
        }
        return Collections.unmodifiableList(recipes);
    }

    private final String sub;

    @JsonCreator
    public SparkCompileBreakRewrite(@JsonProperty("sub") String sub) {
        this.sub = sub;
    }

    public String getSub() {
        return sub;
    }

    @Override
    public String getName() {
        return SparkCompileBreakRewrite.class.getName() + "." + sub;
    }

    @Override
    public String getDisplayName() {
        return "Rewrite Spark 3.5.5 / JDK 17 compile break: " + sub;
    }

    @Override
    public String getDescription() {
        return "Rewrites source that compiles on Spark 2.4.8 / JDK 8 and not on Spark 3.5.5 / JDK 17 to its exact " +
                "3.5.5 form (" + sub + ").";
    }

    // ---- which sites are rewritten (shared with SparkCompileBreakDetect) ----

    /** True when this row's rewrite handles the call (unvisited, at {@code cursor}). */
    static boolean handles(String sub, J.MethodInvocation m) {
        switch (sub) {
            case "MulticlassMetricsNoLabel":
            case "GbtNumTrees":
            case "StageInfoAttemptId":
            case "MlpLayers":
                return renamed(sub, m) != null;
            case "KMeansRuns":
                return (SET_RUNS.matches(m) && isPure(m.getArguments().get(0))) ||
                        (TRAIN_RUNS.matches(m) && isPure(m.getArguments().get(3)));
            case "MlReadWriteContext":
                return CONTEXT.matches(m) && m.getSelect() != null;
            case "WindowBoundaryFunctions":
                return RANGE_BETWEEN.matches(m) && m.getSelect() != null &&
                        boundary(m.getArguments().get(0)) != null && boundary(m.getArguments().get(1)) != null;
            case "UnionWithList":
                return (UNION_RDD.matches(m) || UNION_STREAM.matches(m)) && m.getSelect() != null &&
                        m.getArguments().size() == 2 && arrayType(m.getArguments().get(0).getType(), new LinkedHashSet<>()) != null;
            case "FlatMapValuesIterable":
                return FLAT_MAP_VALUES.matches(m) && m.getArguments().size() == 1 && m.getArguments().get(0) instanceof J.Lambda &&
                        returned((J.Lambda) m.getArguments().get(0)).stream().allMatch(SparkCompileBreakRewrite::typedWithoutTarget);
            case "SunMiscBase64":
                return encodedBytes(m) != null;
            default:
                return false;
        }
    }

    /** The boundary function a rangeBetween argument calls, or null. */
    static String boundary(Expression e) {
        if (e instanceof J.MethodInvocation) {
            J.MethodInvocation b = (J.MethodInvocation) e;
            if (BOUNDARY.matches(b) && BOUNDARIES.contains(b.getSimpleName())) {
                return b.getSimpleName();
            }
        }
        return null;
    }

    /** A boundary call that is a bound of a rangeBetween the rewrite handles. */
    static boolean handledBoundary(J.MethodInvocation m, Cursor cursor) {
        Object parent = cursor.getParentTreeCursor().getValue();
        return boundary(m) != null && parent instanceof J.MethodInvocation &&
                handles("WindowBoundaryFunctions", (J.MethodInvocation) parent);
    }

    private static String renamed(String sub, J.MethodInvocation m) {
        String[] row = RENAMES.get(sub);
        for (int i = 1; i < row.length; i++) {
            if (new MethodMatcher(row[i], true).matches(m)) {
                return row[0];
            }
        }
        return null;
    }

    // A literal or a variable: dropping it drops no side effect.
    private static boolean isPure(Expression e) {
        return e instanceof J.Literal || e instanceof J.Identifier;
    }

    // `new BASE64Encoder().encode(b)` / `encodeBuffer(b)` with b a byte[]: b, else null.
    static Expression encodedBytes(J.MethodInvocation m) {
        if (!("encode".equals(m.getSimpleName()) || "encodeBuffer".equals(m.getSimpleName())) ||
                !(m.getSelect() instanceof J.NewClass) || m.getArguments().size() != 1) {
            return null;
        }
        J.NewClass n = (J.NewClass) m.getSelect();
        Expression b = m.getArguments().get(0);
        boolean encoder = n.getClazz() != null && n.getArguments().stream().allMatch(a -> a instanceof J.Empty) &&
                ("BASE64Encoder".equals(n.getClazz().toString().trim()) || "sun.misc.BASE64Encoder".equals(n.getClazz().toString().trim()));
        boolean bytes = b.getType() instanceof JavaType.Array &&
                ((JavaType.Array) b.getType()).getElemType() == JavaType.Primitive.Byte;
        return encoder && bytes ? b : null;
    }

    /** True when every use of the named type in the file is rewritten, so its import goes. */
    static boolean importGoes(String sub, J.CompilationUnit cu) {
        boolean encoder = "SunMiscBase64".equals(sub);
        String name = encoder ? "BASE64Encoder" : "InterfaceStability";
        int[] uses = {0, 0};
        new JavaIsoVisitor<int[]>() {
            @Override
            public J.Import visitImport(J.Import i, int[] p) {
                return i;
            }

            @Override
            public J.Identifier visitIdentifier(J.Identifier id, int[] p) {
                if (name.equals(id.getSimpleName())) {
                    p[0]++;
                }
                return id;
            }

            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation m, int[] p) {
                if (encoder && encodedBytes(m) != null) {
                    p[1]++;
                }
                return super.visitMethodInvocation(m, p);
            }

            @Override
            public J.Annotation visitAnnotation(J.Annotation a, int[] p) {
                if (!encoder && stabilityAnnotation(a) != null) {
                    p[1]++;
                }
                return super.visitAnnotation(a, p);
            }
        }.visit(cu, uses);
        return uses[0] == uses[1];
    }

    // `@InterfaceStability.X` (also fully qualified), X in Stable/Evolving/Unstable: X, else null.
    static String stabilityAnnotation(J.Annotation a) {
        if (a.getAnnotationType() instanceof J.FieldAccess) {
            J.FieldAccess fa = (J.FieldAccess) a.getAnnotationType();
            String target = fa.getTarget().toString().trim();
            if ((target.equals("InterfaceStability") || target.equals("org.apache.spark.annotation.InterfaceStability")) &&
                    ANNOTATIONS.contains(fa.getSimpleName())) {
                return fa.getSimpleName();
            }
        }
        return null;
    }

    // The array type to build for union's first argument, adding the imports it
    // needs: `JavaRDD<String>` (cast) over the raw `JavaRDD`. Null when a type
    // argument cannot be written (a type variable, a wildcard).
    private static String[] arrayType(JavaType t, Set<String> imports) {
        String generic = typeText(t, imports);
        if (generic == null) {
            return null;
        }
        String raw = t instanceof JavaType.Parameterized ? ((JavaType.Parameterized) t).getClassName() : generic;
        return new String[]{generic, raw};
    }

    private static String typeText(JavaType t, Set<String> imports) {
        if (t instanceof JavaType.Parameterized) {
            JavaType.Parameterized p = (JavaType.Parameterized) t;
            List<String> args = new ArrayList<>();
            for (JavaType a : p.getTypeParameters()) {
                String s = typeText(a, imports);
                if (s == null) {
                    return null;
                }
                args.add(s);
            }
            imports.add(p.getFullyQualifiedName());
            return p.getClassName() + "<" + String.join(", ", args) + ">";
        }
        if (t instanceof JavaType.Class) {
            JavaType.Class c = (JavaType.Class) t;
            if (c.getOwningClass() != null) {
                return null;
            }
            imports.add(c.getFullyQualifiedName());
            return c.getClassName();
        }
        return null;
    }

    private static boolean needsParens(Expression e) {
        return !(e instanceof J.Identifier || e instanceof J.MethodInvocation || e instanceof J.FieldAccess ||
                e instanceof J.NewClass || e instanceof J.Parentheses || e instanceof J.Literal);
    }

    // What a lambda returns: its body, or each return of its block (not those
    // of a nested lambda or class).
    private static List<Expression> returned(J.Lambda lambda) {
        List<Expression> out = new ArrayList<>();
        if (lambda.getBody() instanceof Expression) {
            out.add((Expression) lambda.getBody());
            return out;
        }
        new JavaIsoVisitor<List<Expression>>() {
            @Override
            public J.Return visitReturn(J.Return r, List<Expression> p) {
                p.add(r.getExpression());
                return r;
            }

            @Override
            public J.Lambda visitLambda(J.Lambda l, List<Expression> p) {
                return l;
            }

            @Override
            public J.NewClass visitNewClass(J.NewClass n, List<Expression> p) {
                return n;
            }
        }.visit(lambda.getBody(), out);
        return out;
    }

    // An expression whose type does not come from where it is returned: not
    // Collections.emptyList() or new ArrayList<>(), which `.iterator()` would
    // type as Object.
    private static boolean typedWithoutTarget(Expression e) {
        if (e == null) {
            return false;
        }
        if (e instanceof J.MethodInvocation) {
            J.MethodInvocation m = (J.MethodInvocation) e;
            JavaType.Method type = m.getMethodType();
            if (type == null) {
                return false;
            }
            // A generic method called with no arguments and no type arguments.
            boolean generic = type.getDeclaringType().getMethods().stream()
                    .anyMatch(d -> d.getName().equals(type.getName()) && !d.getDeclaredFormalTypeNames().isEmpty());
            boolean noArgs = m.getArguments().isEmpty() || m.getArguments().get(0) instanceof J.Empty;
            return !(generic && noArgs && m.getTypeParameters() == null);
        }
        if (e instanceof J.NewClass && ((J.NewClass) e).getClazz() instanceof J.ParameterizedType) {
            List<Expression> args = ((J.ParameterizedType) ((J.NewClass) e).getClazz()).getTypeParameters();
            return args != null && args.stream().noneMatch(a -> a instanceof J.Empty);
        }
        return true;
    }

    // `e.iterator()`, parenthesising e when it is not a primary expression.
    private static J.MethodInvocation iteratorOf(Expression e) {
        Expression target = e.withPrefix(Space.EMPTY);
        if (needsParens(e)) {
            target = new J.Parentheses<>(Tree.randomId(), Space.EMPTY, Markers.EMPTY, JRightPadded.build(target));
        }
        return new J.MethodInvocation(Tree.randomId(), e.getPrefix(), Markers.EMPTY, JRightPadded.build(target), null,
                new J.Identifier(Tree.randomId(), Space.EMPTY, Markers.EMPTY, Collections.emptyList(), "iterator", null, null),
                JContainer.build(Collections.singletonList(JRightPadded.build(
                        new J.Empty(Tree.randomId(), Space.EMPTY, Markers.EMPTY)))), null);
    }

    // A replacement for a whole call: parenthesised unless it stands alone (a
    // variable's value, an argument, a return or an assignment's value).
    private static String inContext(String text, Cursor cursor) {
        Object parent = cursor.getParentTreeCursor().getValue();
        boolean alone = parent instanceof J.VariableDeclarations.NamedVariable || parent instanceof J.Return ||
                parent instanceof J.Assignment || parent instanceof J.Lambda ||
                (parent instanceof J.MethodInvocation && ((J.MethodInvocation) parent).getSelect() != cursor.getValue()) ||
                parent instanceof J.NewClass;
        return alone ? text : "(" + text + ")";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        if (!SUBS.contains(sub)) {
            return TreeVisitor.noop();
        }
        return new JavaVisitor<ExecutionContext>() {
            private boolean importGoes;
            // The Base64 helpers the rewritten calls in this file need, by method name.
            private final Set<String> helpers = new LinkedHashSet<>();

            @Override
            public J visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
                importGoes = ("SunMiscBase64".equals(sub) || "InterfaceStability".equals(sub)) && importGoes(sub, cu);
                J.CompilationUnit c = (J.CompilationUnit) super.visitCompilationUnit(cu, ctx);
                if (importGoes && c != cu) {
                    String fqn = "SunMiscBase64".equals(sub) ? "sun.misc.BASE64Encoder" : "org.apache.spark.annotation.InterfaceStability";
                    List<J.Import> kept = new ArrayList<>();
                    for (J.Import i : c.getImports()) {
                        if (!i.getTypeName().equals(fqn) && !i.toString().trim().equals("import " + fqn)) {
                            kept.add(i);
                        }
                    }
                    c = c.withImports(kept);
                }
                return c;
            }

            @Override
            public J visitClassDeclaration(J.ClassDeclaration classDecl, ExecutionContext ctx) {
                J c = super.visitClassDeclaration(classDecl, ctx);
                boolean topLevel = getCursor().getParentTreeCursor().getValue() instanceof J.CompilationUnit;
                if (!topLevel || helpers.isEmpty() || !(c instanceof J.ClassDeclaration)) {
                    return c;
                }
                J.ClassDeclaration cd = (J.ClassDeclaration) c;
                for (String helper : helpers) {
                    boolean declared = cd.getBody().getStatements().stream().anyMatch(st ->
                            st instanceof J.MethodDeclaration && helper.equals(((J.MethodDeclaration) st).getSimpleName()));
                    if (!declared) {
                        String call = helper.equals(base64Helper("encode")) ? "encode" : "encodeBuffer";
                        cd = JavaTemplate.apply("private static String " + helper + "(byte[] bytes) {\n" +
                                        "    return " + base64Encoding("bytes", call) + ";\n}",
                                new Cursor(getCursor().getParentOrThrow(), cd), cd.getBody().getCoordinates().lastStatement());
                    }
                }
                helpers.clear();
                return cd;
            }

            @Override
            public J visitAnnotation(J.Annotation annotation, ExecutionContext ctx) {
                J.Annotation a = (J.Annotation) super.visitAnnotation(annotation, ctx);
                String x = "InterfaceStability".equals(sub) ? stabilityAnnotation(a) : null;
                if (x == null) {
                    return a;
                }
                maybeAddImport("org.apache.spark.annotation." + x, null, false);
                J.FieldAccess fa = (J.FieldAccess) a.getAnnotationType();
                return a.withAnnotationType(fa.getName().withPrefix(fa.getPrefix())
                        .withType(JavaType.ShallowClass.build("org.apache.spark.annotation." + x)));
            }

            @Override
            public J visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                boolean rewrite = handles(sub, method);
                J j = super.visitMethodInvocation(method, ctx);
                if (!rewrite || !(j instanceof J.MethodInvocation)) {
                    return j;
                }
                J.MethodInvocation m = (J.MethodInvocation) j;
                List<Expression> args = m.getArguments();
                switch (sub) {
                    case "KMeansRuns": {
                        if (SET_RUNS.matches(method)) {
                            return m.getSelect().withPrefix(m.getPrefix());
                        }
                        List<Expression> kept = new ArrayList<>(args);
                        kept.remove(3);
                        JavaType.Method type = m.getMethodType();
                        if (type != null) {
                            List<String> names = new ArrayList<>(type.getParameterNames());
                            List<JavaType> types = new ArrayList<>(type.getParameterTypes());
                            names.remove(3);
                            types.remove(3);
                            type = type.withParameterNames(names).withParameterTypes(types);
                        }
                        return m.withArguments(kept).withMethodType(type);
                    }
                    case "MlReadWriteContext":
                        return JavaTemplate.apply("#{any()}.session(#{any()}.sparkSession())", getCursor(),
                                m.getCoordinates().replace(), m.getSelect(), args.get(0));
                    case "WindowBoundaryFunctions":
                        maybeAddImport(WINDOW, null, false);
                        for (String b : BOUNDARIES) {
                            maybeRemoveImport("org.apache.spark.sql.functions." + b);
                        }
                        return JavaTemplate.apply("#{any()}.rangeBetween(Window." + boundary(args.get(0)) + "(), Window." +
                                        boundary(args.get(1)) + "())", getCursor(), m.getCoordinates().replace(), m.getSelect());
                    case "UnionWithList": {
                        Set<String> imports = new LinkedHashSet<>();
                        String[] type = arrayType(method.getArguments().get(0).getType(), imports);
                        imports.add("java.util.stream.Stream");
                        for (String i : imports) {
                            maybeAddImport(i, null, false);
                        }
                        String array = "Stream.concat(Stream.of(#{any()}), #{any()}.stream()).toArray(" + type[1] + "[]::new)";
                        return JavaTemplate.apply("#{any()}.union(" +
                                        (type[0].equals(type[1]) ? array : "(" + type[0] + "[]) " + array) + ")",
                                getCursor(), m.getCoordinates().replace(), m.getSelect(), args.get(0), args.get(1));
                    }
                    case "FlatMapValuesIterable":
                        return m.withArguments(Collections.singletonList(iterators((J.Lambda) args.get(0))));
                    case "SunMiscBase64": {
                        maybeAddImport("java.util.Base64", null, false);
                        if (!(encodedBytes(method) instanceof J.Identifier)) {
                            String helper = base64Helper(method.getSimpleName());
                            helpers.add(helper);
                            return JavaTemplate.apply(helper + "(#{any(byte[])})", getCursor(), m.getCoordinates().replace(),
                                    args.get(0));
                        }
                        return JavaTemplate.apply(inContext(base64Encoding(((J.Identifier) encodedBytes(method)).getSimpleName(),
                                method.getSimpleName()), getCursor()), getCursor(), m.getCoordinates().replace());
                    }
                    default: {
                        String to = renamed(sub, method);
                        JavaType.Method type = m.getMethodType();
                        return m.withName(m.getName().withSimpleName(to))
                                .withMethodType(type == null ? null : type.withName(to));
                    }
                }
            }
        };
    }

    /** What sun's encoder printed for {@code b}, a byte[] variable (see the class comment). */
    private static String base64Encoding(String b, String call) {
        String trailing = "encode".equals(call) ? b + ".length > 0 && " + b + ".length % 57 == 0" : b + ".length > 0";
        return "Base64.getMimeEncoder(76, System.lineSeparator().getBytes()).encodeToString(" + b + ") + (" +
                trailing + " ? System.lineSeparator() : \"\")";
    }

    /** The helper's name for encode / encodeBuffer. */
    private static String base64Helper(String call) {
        return "encode".equals(call) ? "sunBase64Encode" : "sunBase64EncodeBuffer";
    }

    // The lambda with `.iterator()` on what it returns (as `returned` finds it).
    private static J.Lambda iterators(J.Lambda lambda) {
        if (lambda.getBody() instanceof Expression) {
            return lambda.withBody(iteratorOf((Expression) lambda.getBody()));
        }
        return lambda.withBody(new JavaIsoVisitor<Integer>() {
            @Override
            public J.Return visitReturn(J.Return r, Integer p) {
                return r.getExpression() == null ? r : r.withExpression(iteratorOf(r.getExpression()));
            }

            @Override
            public J.Lambda visitLambda(J.Lambda l, Integer p) {
                return l;
            }

            @Override
            public J.NewClass visitNewClass(J.NewClass n, Integer p) {
                return n;
            }
        }.visit(lambda.getBody(), 0));
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SparkCompileBreakRewrite && Objects.equals(((SparkCompileBreakRewrite) o).sub, sub);
    }

    @Override
    public int hashCode() {
        return Objects.hash(SparkCompileBreakRewrite.class, sub);
    }
}
