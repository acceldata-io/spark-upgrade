package com.acceldata.openrewrite.spark;

import org.openrewrite.Cursor;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaParser;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.TypeTree;
import org.openrewrite.java.tree.TypeUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Rewrites the removed v1 accumulator in its common Java shape: a local
 * variable or private field declared {@code Accumulator<Integer|Long|Double>},
 * initialised by {@code jsc.accumulator(0)} (or {@code 0L}, {@code 0.0}, an
 * optional name, or {@code intAccumulator}/{@code doubleAccumulator}), and used
 * only through {@code add(x)} and {@code value()}:
 * <pre>
 *   Accumulator&lt;Integer&gt; bad = jsc.accumulator(0);    LongAccumulator bad = jsc.sc().longAccumulator();
 *   bad.add(1);                                    bad.add(1);
 *   int n = bad.value();                           int n = bad.value().intValue();
 * </pre>
 * Exact: a LongAccumulator's sum cast back to int is the int sum, overflow
 * included, and {@code add} takes the same arguments (an int or Integer widens
 * to long). A Double accumulator becomes a DoubleAccumulator, a Long one a
 * LongAccumulator, with {@code value()} unchanged.
 *
 * <p>Left to {@link SparkAccumulatorV1Detect}: a non-zero initial value (the
 * typed accumulators start at zero), any other use of the variable (passed to
 * a method, {@code localValue()}, a non-private field another file may use),
 * an Integer {@code value()} used as the receiver of another call, and a call
 * that is not a declaration's initialiser.
 */
public class SparkAccumulatorV1Rewrite extends Recipe {

    private static final List<MethodMatcher> FACTORIES = Arrays.asList(
            new MethodMatcher("org.apache.spark.api.java.JavaSparkContext accumulator(..)"),
            new MethodMatcher("org.apache.spark.api.java.JavaSparkContext intAccumulator(..)"),
            new MethodMatcher("org.apache.spark.api.java.JavaSparkContext doubleAccumulator(..)"));

    private static final String LONG = "org.apache.spark.util.LongAccumulator";
    private static final String DOUBLE = "org.apache.spark.util.DoubleAccumulator";

    // Gives the templates' output real type attribution without Spark on the
    // runner's own classpath.
    private static final String[] STUBS = {
            "package org.apache.spark.util;\npublic class LongAccumulator { public Long value() { return null; } }\n",
            "package org.apache.spark.util;\npublic class DoubleAccumulator { public Double value() { return null; } }\n",
            "package org.apache.spark;\nimport org.apache.spark.util.*;\npublic class SparkContext {\n" +
                    "  public LongAccumulator longAccumulator() { return null; }\n" +
                    "  public LongAccumulator longAccumulator(String name) { return null; }\n" +
                    "  public DoubleAccumulator doubleAccumulator() { return null; }\n" +
                    "  public DoubleAccumulator doubleAccumulator(String name) { return null; }\n}\n",
            "package org.apache.spark.api.java;\npublic class JavaSparkContext { public org.apache.spark.SparkContext sc() { return null; } }\n"};

    @Override
    public String getDisplayName() {
        return "Rewrite the removed v1 accumulator to LongAccumulator / DoubleAccumulator";
    }

    @Override
    public String getDescription() {
        return "Accumulator<Integer|Long|Double> x = jsc.accumulator(0[, name]), used only through add and value, " +
                "becomes the typed accumulator sc().longAccumulator([name]) / doubleAccumulator([name]); an Integer " +
                "value() becomes value().intValue(). The same sums.";
    }

    /** The element type ("Integer", "Long", "Double") of each declaration in {@code cu} the rewrite takes, by variable. */
    static Map<JavaType.Variable, String> rewritten(J.CompilationUnit cu) {
        Map<JavaType.Variable, String> candidates = new HashMap<>();
        new JavaIsoVisitor<Integer>() {
            @Override
            public J.VariableDeclarations visitVariableDeclarations(J.VariableDeclarations vd, Integer p) {
                String element = element(vd, getCursor());
                if (element != null) {
                    candidates.put(vd.getVariables().get(0).getVariableType(), element);
                }
                return super.visitVariableDeclarations(vd, p);
            }
        }.visit(cu, 0);
        if (candidates.isEmpty()) {
            return candidates;
        }
        // Every other reference must be `x.add(arg)` or `x.value()`; an Integer value() not the receiver of a call.
        Set<JavaType.Variable> rejected = new HashSet<>();
        new JavaIsoVisitor<Integer>() {
            @Override
            public J.Identifier visitIdentifier(J.Identifier id, Integer p) {
                JavaType.Variable v = id.getFieldType();
                if (v == null || !candidates.containsKey(v)) {
                    return id;
                }
                Object parent = getCursor().getParentTreeCursor().getValue();
                if (parent instanceof J.VariableDeclarations.NamedVariable) {
                    return id;
                }
                boolean ok = false;
                if (parent instanceof J.MethodInvocation && ((J.MethodInvocation) parent).getSelect() == id) {
                    J.MethodInvocation m = (J.MethodInvocation) parent;
                    boolean noArgs = m.getArguments().stream().allMatch(a -> a instanceof J.Empty);
                    if ("add".equals(m.getSimpleName()) && m.getArguments().size() == 1 && !noArgs) {
                        ok = true;
                    } else if ("value".equals(m.getSimpleName()) && noArgs) {
                        Object outer = getCursor().getParentTreeCursor().getParentTreeCursor().getValue();
                        ok = !"Integer".equals(candidates.get(v)) ||
                                !(outer instanceof J.MethodInvocation && ((J.MethodInvocation) outer).getSelect() == m) &&
                                        !(outer instanceof J.FieldAccess);
                    }
                }
                if (!ok) {
                    rejected.add(v);
                }
                return id;
            }
        }.visit(cu, 0);
        rejected.forEach(candidates::remove);
        return candidates;
    }

    /** The element type when {@code vd} declares one accumulator the rewrite can take on its own, else null. */
    private static String element(J.VariableDeclarations vd, Cursor cursor) {
        if (vd.getVariables().size() != 1 || !(vd.getVariables().get(0).getInitializer() instanceof J.MethodInvocation)) {
            return null;
        }
        J.MethodInvocation init = (J.MethodInvocation) vd.getVariables().get(0).getInitializer();
        if (FACTORIES.stream().noneMatch(f -> f.matches(init)) || init.getSelect() == null ||
                init.getArguments().isEmpty() || init.getArguments().size() > 2 || !isZero(init.getArguments().get(0))) {
            return null;
        }
        boolean field = cursor.getParentTreeCursor().getValue() instanceof J.Block &&
                cursor.getParentTreeCursor().getParentTreeCursor().getValue() instanceof J.ClassDeclaration;
        if (field && !vd.hasModifier(J.Modifier.Type.Private)) {
            return null;
        }
        JavaType.Parameterized type = TypeUtils.asParameterized(vd.getType());
        if (type == null || !"org.apache.spark.Accumulator".equals(type.getFullyQualifiedName()) ||
                type.getTypeParameters().size() != 1) {
            return null;
        }
        String element = TypeUtils.asFullyQualified(type.getTypeParameters().get(0)) == null ? null
                : TypeUtils.asFullyQualified(type.getTypeParameters().get(0)).getFullyQualifiedName();
        if ("java.lang.Integer".equals(element)) {
            return "Integer";
        }
        if ("java.lang.Long".equals(element)) {
            return "Long";
        }
        return "java.lang.Double".equals(element) ? "Double" : null;
    }

    private static boolean isZero(Expression e) {
        return e instanceof J.Literal && ((J.Literal) e).getValue() instanceof Number &&
                ((Number) ((J.Literal) e).getValue()).doubleValue() == 0.0;
    }

    /** True when {@code m} is the initialiser of a declaration the rewrite takes (for the detector). */
    static boolean handles(J.MethodInvocation m, Map<JavaType.Variable, String> rewritten, Cursor cursor) {
        Object parent = cursor.getParentTreeCursor().getValue();
        return parent instanceof J.VariableDeclarations.NamedVariable &&
                rewritten.containsKey(((J.VariableDeclarations.NamedVariable) parent).getVariableType());
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaVisitor<ExecutionContext>() {
            private Map<JavaType.Variable, String> rewritten = new HashMap<>();

            @Override
            public J visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
                rewritten = rewritten(cu);
                if (rewritten.isEmpty()) {
                    return cu;
                }
                J.CompilationUnit c = (J.CompilationUnit) super.visitCompilationUnit(cu, ctx);
                // The rewritten variables' references still carry the old type, so
                // maybeRemoveImport would keep it: drop it when no code names it.
                boolean named = new JavaIsoVisitor<boolean[]>() {
                    @Override
                    public J.Import visitImport(J.Import i, boolean[] p) {
                        return i;
                    }

                    @Override
                    public J.Identifier visitIdentifier(J.Identifier id, boolean[] p) {
                        p[0] |= "Accumulator".equals(id.getSimpleName());
                        return id;
                    }
                }.reduce(c, new boolean[1])[0];
                return named ? c : c.withImports(c.getImports().stream()
                        .filter(i -> !"org.apache.spark.Accumulator".equals(i.getTypeName())).collect(Collectors.toList()));
            }

            @Override
            public J visitVariableDeclarations(J.VariableDeclarations multiVariable, ExecutionContext ctx) {
                String element = rewritten.get(multiVariable.getVariables().get(0).getVariableType());
                J j = super.visitVariableDeclarations(multiVariable, ctx);
                if (element == null || !(j instanceof J.VariableDeclarations)) {
                    return j;
                }
                J.VariableDeclarations vd = (J.VariableDeclarations) j;
                String fqn = "Double".equals(element) ? DOUBLE : LONG;
                maybeAddImport(fqn, null, false);
                JavaType.ShallowClass type = JavaType.ShallowClass.build(fqn);
                J.VariableDeclarations.NamedVariable v = vd.getVariables().get(0);
                v = v.withVariableType(v.getVariableType() == null ? null : v.getVariableType().withType(type))
                        .withName(v.getName().withType(type));
                TypeTree typeName = ((J.Identifier) TypeTree.build(fqn.substring(fqn.lastIndexOf('.') + 1))).withType(type)
                        .withPrefix(vd.getTypeExpression().getPrefix());
                return vd.withTypeExpression(typeName).withVariables(Collections.singletonList(v));
            }

            @Override
            public J visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                boolean initializer = handles(method, rewritten, getCursor());
                J j = super.visitMethodInvocation(method, ctx);
                if (initializer) {
                    J.VariableDeclarations.NamedVariable named = getCursor().getParentTreeCursor().getValue();
                    String factory = "Double".equals(rewritten.get(named.getVariableType())) ? "doubleAccumulator" : "longAccumulator";
                    List<Expression> args = method.getArguments();
                    JavaTemplate.Builder template = JavaTemplate.builder("#{any(org.apache.spark.api.java.JavaSparkContext)}.sc()." +
                            factory + (args.size() == 2 ? "(#{any(java.lang.String)})" : "()"))
                            .javaParser(JavaParser.fromJavaVersion().dependsOn(STUBS));
                    return args.size() == 2
                            ? template.build().apply(getCursor(), method.getCoordinates().replace(), method.getSelect(), args.get(1))
                            : template.build().apply(getCursor(), method.getCoordinates().replace(), method.getSelect());
                }
                if (!"value".equals(method.getSimpleName()) || !(method.getSelect() instanceof J.Identifier) ||
                        !"Integer".equals(rewritten.get(((J.Identifier) method.getSelect()).getFieldType()))) {
                    return j;
                }
                return JavaTemplate.apply("#{any()}.intValue()", getCursor(), method.getCoordinates().replace(), j);
            }
        };
    }
}
