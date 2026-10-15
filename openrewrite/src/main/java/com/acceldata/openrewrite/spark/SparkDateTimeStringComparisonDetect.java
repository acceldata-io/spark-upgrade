package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.internal.lang.Nullable;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Java-side counterpart to the Scalafix {@code DateTimeStringComparisonDetect}
 * rule: from Spark 3.0 a comparison between a date/timestamp and a string casts
 * the STRING to date/timestamp, where 2.4 cast the date/timestamp to string and
 * compared text (a timestamp under = and <=> excepted). Column types are not
 * visible, so it keys on the other operand: a {@code Column} comparison
 * ({@code equalTo}, {@code notEqual}, {@code gt}, {@code lt}, {@code geq},
 * {@code leq}, {@code eqNullSafe}, {@code between}) with a date-shaped string
 * literal -- directly, via {@code lit("...")}, or through a variable
 * initialized to either in the same file.
 *
 * <p>Only the literal/operator pairs a probe showed can change are flagged, the
 * same predicate as the sqlfluff rule's {@code date_string_comparison_changes}
 * (spark-upgrade/sql): a literal that is not a zero-padded yyyy-MM-dd changes
 * under every operator; a zero-padded one only under {@code gt} and
 * {@code leq} (the column as receiver) and as {@code between}'s upper bound.
 * Embedded SQL text is the shared SQL linter's.
 *
 * <p>Feeds {@code spark.sql.legacy.typeCoercion.datetimeToString.enabled} (Tier 2).
 */
public class SparkDateTimeStringComparisonDetect extends Recipe {

    // Each comparison as the SQL operator it is, the receiver on the left.
    private static final Map<String, String> OPERATORS = new HashMap<>();
    static {
        OPERATORS.put("equalTo", "=");
        OPERATORS.put("notEqual", "<>");
        OPERATORS.put("lt", "<");
        OPERATORS.put("leq", "<=");
        OPERATORS.put("gt", ">");
        OPERATORS.put("geq", ">=");
        OPERATORS.put("eqNullSafe", "<=>");
    }
    // The operator seen from the column's side when the literal is the receiver.
    private static final Map<String, String> MIRRORED = new HashMap<>();
    static {
        MIRRORED.put("<", ">");
        MIRRORED.put(">", "<");
        MIRRORED.put("<=", ">=");
        MIRRORED.put(">=", "<=");
    }
    private static final MethodMatcher COLUMN_METHOD = new MethodMatcher("org.apache.spark.sql.Column *(..)");
    private static final MethodMatcher LIT = new MethodMatcher("org.apache.spark.sql.functions lit(..)");
    private static final Pattern DATE_STRING =
            Pattern.compile("\\d{4}-\\d{1,2}-\\d{1,2}(?:[ T]\\d{1,2}:\\d{1,2}(?::\\d{1,2}(?:\\.\\d{1,9})?)?)?");
    private static final Pattern CANONICAL_DATE = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
    private static final List<String> CANONICAL_CHANGES = Arrays.asList(">", "<=", "BETWEEN_HI");

    /** Whether comparing a date or timestamp column to {@code literal} with {@code operator} can answer differently on 3.x. */
    static boolean changes(String literal, String operator) {
        return DATE_STRING.matcher(literal).matches() &&
                (!CANONICAL_DATE.matcher(literal).matches() || CANONICAL_CHANGES.contains(operator));
    }

    @Override
    public String getDisplayName() {
        return "Detect a Column comparison with a date-shaped string";
    }

    @Override
    public String getDescription() {
        return "A Column comparison with a date-shaped string literal whose result can differ on Spark 3.x, which " +
                "casts the string to date/timestamp instead of the date/timestamp to string.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            /** Initializer of every variable in the file, by name. */
            private final Map<String, Expression> initializers = new HashMap<>();

            @Override
            public J.CompilationUnit visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
                initializers.clear();
                new JavaIsoVisitor<Map<String, Expression>>() {
                    @Override
                    public J.VariableDeclarations.NamedVariable visitVariable(J.VariableDeclarations.NamedVariable v, Map<String, Expression> acc) {
                        if (v.getInitializer() != null) {
                            acc.put(v.getSimpleName(), v.getInitializer());
                        }
                        return super.visitVariable(v, acc);
                    }
                }.visit(cu, initializers);
                return super.visitCompilationUnit(cu, ctx);
            }

            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (!COLUMN_METHOD.matches(m)) {
                    return m;
                }
                List<Expression> args = m.getArguments();
                boolean changed;
                if ("between".equals(m.getSimpleName()) && args.size() == 2) {
                    changed = changes(args.get(0), "BETWEEN_LO") || changes(args.get(1), "BETWEEN_HI");
                } else if (OPERATORS.containsKey(m.getSimpleName()) && args.size() == 1) {
                    String op = OPERATORS.get(m.getSimpleName());
                    changed = changes(args.get(0), op) || changes(m.getSelect(), MIRRORED.getOrDefault(op, op));
                } else {
                    changed = false;
                }
                if (changed) {
                    return SearchResult.found(
                            m,
                            "A column is compared with a date-shaped string in a way whose result can change. If the " +
                                    "column is a date or timestamp, Spark 3.0 casts the string to date/timestamp, where " +
                                    "2.4 cast the column to string and compared text: ts > '2020-01-01' kept midnight " +
                                    "rows on 2.4 and drops them on 3.x, and d = '2020-1-1' matched nothing on 2.4. The " +
                                    "tool injects spark.sql.legacy.typeCoercion.datetimeToString.enabled=true to keep " +
                                    "2.4's behaviour; its cost is that a date partition column compared to a string is " +
                                    "no longer pruned."
                    );
                }
                return m;
            }

            private boolean changes(@Nullable Expression operand, String operator) {
                String literal = stringLiteral(operand, true);
                return literal != null && SparkDateTimeStringComparisonDetect.changes(literal, operator);
            }

            private @Nullable String stringLiteral(@Nullable Expression e, boolean followVariable) {
                if (e instanceof J.Literal && ((J.Literal) e).getValue() instanceof String) {
                    return (String) ((J.Literal) e).getValue();
                }
                if (e instanceof J.MethodInvocation && LIT.matches((J.MethodInvocation) e)) {
                    List<Expression> args = ((J.MethodInvocation) e).getArguments();
                    return args.size() == 1 ? stringLiteral(args.get(0), false) : null;
                }
                if (e instanceof J.Identifier && followVariable) {
                    return stringLiteral(initializers.get(((J.Identifier) e).getSimpleName()), false);
                }
                return null;
            }
        };
    }
}
