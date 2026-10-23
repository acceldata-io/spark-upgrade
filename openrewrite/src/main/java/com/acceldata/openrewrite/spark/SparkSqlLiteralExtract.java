package com.acceldata.openrewrite.spark;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.Statement;
import org.openrewrite.marker.SearchResult;

/**
 * Not a migration-guide finding on its own -- a plumbing recipe that feeds
 * a caller's Java-side embedded-SQL (Family K) lint, the Java counterpart to
 * the Scalafix rule {@code SparkSqlExtract}.
 *
 * <p>It marks the argument of every {@code SparkSession#sql}/{@code SQLContext#sql}
 * call with a {@code SearchResult} whose description carries the SQL text, so
 * {@code Runner}'s existing hunk parsing gives the caller the exact file and line,
 * and the text rides along in the finding's {@code diff_snippet} as the
 * {@code /*~~(<description>)~~>*&#47;} marker comment. The description is:
 * <ul>
 *   <li>a string literal: its SQL, as is;</li>
 *   <li>anything else: {@code @@template:<n>@@} and the SQL, with each of its n
 *       run-time parts written {@code __param__} -- or {@code 0} where a format
 *       conversion says it is a number ({@code %d}), so {@code LIMIT %d} still
 *       parses -- a {@code +} concatenation, a {@code String.format}, or a variable
 *       this file initialises once and never assigns again (n is 0 when its whole
 *       text is known);</li>
 *   <li>{@code @@dynamic@@} when no text is known (a parameter, a method's result).</li>
 * </ul>
 *
 * <p>Known, documented limitation: SQL containing the substring
 * {@code )~~>*&#47;} would truncate the recovered text at that point --
 * vanishingly rare in real SQL text.
 */
public class SparkSqlLiteralExtract extends Recipe {

    private static final MethodMatcher SPARK_SESSION_SQL =
            new MethodMatcher("org.apache.spark.sql.SparkSession sql(java.lang.String)");
    private static final MethodMatcher SQL_CONTEXT_SQL =
            new MethodMatcher("org.apache.spark.sql.SQLContext sql(java.lang.String)");
    private static final MethodMatcher STRING_FORMAT =
            new MethodMatcher("java.lang.String format(java.lang.String, ..)");

    static final String PARAM = "__param__";
    private static final int MAX_DEPTH = 3;
    // A java.util.Formatter specifier.
    private static final Pattern FORMAT_SPEC = Pattern.compile("%(\\d+\\$)?[-#+ 0,(<]*\\d*(\\.\\d+)?[a-zA-Z%]");

    @Override
    public String getDisplayName() {
        return "Extract the SQL passed to sql(...) for the Family K sqlfluff pipeline";
    }

    @Override
    public String getDescription() {
        return "Internal plumbing recipe: marks each SQL argument passed to SparkSession#sql/SQLContext#sql with its " +
                "text -- literal, template with placeholders, or dynamic -- so spark-migrate-cli's Java-side " +
                "embedded-SQL analyzer can lint it and report its file/line.";
    }

    /** SQL text known at compile time, with {@code holes} parts known only at run time. */
    private static final class Sql {
        final String text;
        final int holes;

        Sql(String text, int holes) {
            this.text = text;
            this.holes = holes;
        }

        Sql plus(Sql o) {
            return new Sql(text + o.text, holes + o.holes);
        }
    }

    private static final Sql HOLE = new Sql(PARAM, 1);

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            // Variables initialised in this file, and those assigned again.
            private final Map<String, Expression> initialisers = new HashMap<>();
            private final Set<String> reassigned = new HashSet<>();
            // `for (String q : DDL) spark.sql(q);` over statements this file spells out: each statement is
            // marked where it is written, and the loop variable's call is not dynamic (otherwise a
            // CHAR(2) column in a list of DDL strings is never linted).
            private final Set<java.util.UUID> loopCalls = new HashSet<>();
            private final Set<java.util.UUID> statements = new HashSet<>();

            @Override
            public J.CompilationUnit visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
                initialisers.clear();
                reassigned.clear();
                loopCalls.clear();
                statements.clear();
                new JavaIsoVisitor<Integer>() {
                    @Override
                    public J.VariableDeclarations.NamedVariable visitVariable(J.VariableDeclarations.NamedVariable v, Integer p) {
                        if (v.getInitializer() != null && v.getVariableType() != null) {
                            initialisers.put(key(v.getVariableType()), v.getInitializer());
                        }
                        return super.visitVariable(v, p);
                    }

                    @Override
                    public J.Assignment visitAssignment(J.Assignment a, Integer p) {
                        markAssigned(a.getVariable());
                        return super.visitAssignment(a, p);
                    }

                    @Override
                    public J.AssignmentOperation visitAssignmentOperation(J.AssignmentOperation a, Integer p) {
                        markAssigned(a.getVariable());
                        return super.visitAssignmentOperation(a, p);
                    }
                }.visit(cu, 0);
                new JavaIsoVisitor<Integer>() {
                    @Override
                    public J.ForEachLoop visitForEachLoop(J.ForEachLoop loop, Integer p) {
                        Statement declared = loop.getControl().getVariable();
                        J.VariableDeclarations.NamedVariable v = declared instanceof J.VariableDeclarations
                                ? ((J.VariableDeclarations) declared).getVariables().get(0) : null;
                        List<Expression> elements = v == null || v.getVariableType() == null ? null : elements(loop.getControl().getIterable(), 0);
                        List<java.util.UUID> calls = elements == null ? Collections.emptyList()
                                : sqlCallsOn(loop.getBody(), v.getSimpleName());
                        if (!calls.isEmpty()) {
                            loopCalls.addAll(calls);
                            for (Expression e : elements) {
                                if (e instanceof J.Literal && ((J.Literal) e).getValue() instanceof String) {
                                    statements.add(e.getId());
                                }
                            }
                        }
                        return super.visitForEachLoop(loop, p);
                    }
                }.visit(cu, 0);
                return super.visitCompilationUnit(cu, ctx);
            }

            /** The elements of an array or list this file spells out: {@code {..}}, {@code new String[]{..}},
             * {@code Arrays.asList(..)}, {@code List.of(..)}, or a variable initialised to one. */
            private List<Expression> elements(Expression e, int depth) {
                if (e instanceof J.NewArray) {
                    return ((J.NewArray) e).getInitializer();
                }
                if (e instanceof J.MethodInvocation) {
                    J.MethodInvocation m = (J.MethodInvocation) e;
                    String name = m.getSimpleName();
                    String owner = m.getSelect() instanceof J.Identifier ? ((J.Identifier) m.getSelect()).getSimpleName()
                            : m.getSelect() instanceof J.FieldAccess ? ((J.FieldAccess) m.getSelect()).getSimpleName() : "";
                    boolean factory = "asList".equals(name) && "Arrays".equals(owner)
                            || "of".equals(name) && ("List".equals(owner) || "Set".equals(owner));
                    return factory ? m.getArguments() : null;
                }
                if (e instanceof J.Identifier && depth < MAX_DEPTH) {
                    JavaType.Variable v = ((J.Identifier) e).getFieldType();
                    if (v != null && !reassigned.contains(key(v)) && initialisers.containsKey(key(v))) {
                        return elements(initialisers.get(key(v)), depth + 1);
                    }
                }
                return null;
            }

            /** The {@code sql(...)} calls in {@code body} passed the loop variable {@code variable}. */
            private List<java.util.UUID> sqlCallsOn(Statement body, String variable) {
                List<java.util.UUID> found = new java.util.ArrayList<>();
                new JavaIsoVisitor<Integer>() {
                    @Override
                    public J.MethodInvocation visitMethodInvocation(J.MethodInvocation m, Integer p) {
                        if ((SPARK_SESSION_SQL.matches(m) || SQL_CONTEXT_SQL.matches(m)) && m.getArguments().size() == 1
                                && m.getArguments().get(0) instanceof J.Identifier) {
                            if (((J.Identifier) m.getArguments().get(0)).getSimpleName().equals(variable)) {
                                found.add(m.getId());
                            }
                        }
                        return super.visitMethodInvocation(m, p);
                    }
                }.visit(body, 0);
                return found;
            }

            @Override
            public J.Literal visitLiteral(J.Literal literal, ExecutionContext ctx) {
                J.Literal l = super.visitLiteral(literal, ctx);
                return statements.contains(l.getId()) ? SearchResult.found(l, (String) l.getValue()) : l;
            }

            private void markAssigned(Expression target) {
                JavaType.Variable v = target instanceof J.Identifier ? ((J.Identifier) target).getFieldType()
                        : target instanceof J.FieldAccess ? ((J.FieldAccess) target).getName().getFieldType() : null;
                if (v != null) {
                    reassigned.add(key(v));
                }
            }

            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (!(SPARK_SESSION_SQL.matches(m) || SQL_CONTEXT_SQL.matches(m)) || m.getArguments().size() != 1) {
                    return m;
                }
                Expression arg = m.getArguments().get(0);
                if (loopCalls.contains(m.getId())) {
                    return m; // its statements are marked where they are written
                }
                String description;
                if (arg instanceof J.Literal && ((J.Literal) arg).getValue() instanceof String) {
                    description = (String) ((J.Literal) arg).getValue();
                } else {
                    Sql sql = read(arg, 0);
                    description = sql == null || sql.text.replace(PARAM, "").trim().isEmpty()
                            ? "@@dynamic@@"
                            : "@@template:" + sql.holes + "@@" + sql.text;
                }
                return m.withArguments(Collections.singletonList(SearchResult.found(arg, description)));
            }

            /** The text of {@code e}, or null when nothing of it is known. */
            private Sql read(Expression e, int depth) {
                if (e instanceof J.Parentheses) {
                    Object tree = ((J.Parentheses<?>) e).getTree();
                    return tree instanceof Expression ? read((Expression) tree, depth) : null;
                }
                if (e instanceof J.Literal) {
                    Object v = ((J.Literal) e).getValue();
                    return v instanceof String ? new Sql((String) v, 0) : null;
                }
                if (e instanceof J.Binary && ((J.Binary) e).getOperator() == J.Binary.Type.Addition) {
                    J.Binary b = (J.Binary) e;
                    Sql l = read(b.getLeft(), depth);
                    Sql r = read(b.getRight(), depth);
                    // String concatenation only: one side must hold some text.
                    if (l == null && r == null) {
                        return null;
                    }
                    return (l != null ? l : known(b.getLeft())).plus(r != null ? r : known(b.getRight()));
                }
                if (e instanceof J.MethodInvocation && STRING_FORMAT.matches((J.MethodInvocation) e)) {
                    List<Expression> args = ((J.MethodInvocation) e).getArguments();
                    Sql fmt = read(args.get(0), depth);
                    return fmt == null ? null : format(fmt);
                }
                if (e instanceof J.Identifier && depth < MAX_DEPTH) {
                    JavaType.Variable v = ((J.Identifier) e).getFieldType();
                    if (v != null && !reassigned.contains(key(v)) && initialisers.containsKey(key(v))) {
                        return read(initialisers.get(key(v)), depth + 1);
                    }
                }
                return null;
            }

            /** A concatenated part that is not text: a number's digits, else a hole. */
            private Sql known(Expression e) {
                if (e instanceof J.Literal && ((J.Literal) e).getValue() instanceof Number) {
                    return new Sql(String.valueOf(((J.Literal) e).getValue()), 0);
                }
                return HOLE;
            }
        };
    }

    private static Sql format(Sql fmt) {
        Matcher m = FORMAT_SPEC.matcher(fmt.text);
        StringBuffer out = new StringBuffer();
        int holes = fmt.holes;
        while (m.find()) {
            char conversion = m.group().charAt(m.group().length() - 1);
            String with = conversion == '%' ? "%" : conversion == 'n' ? "\n"
                    : "doxXeEfgGa".indexOf(conversion) >= 0 ? "0" : PARAM;
            if (conversion != '%' && conversion != 'n') {
                holes++;
            }
            m.appendReplacement(out, Matcher.quoteReplacement(with));
        }
        m.appendTail(out);
        return new Sql(out.toString(), holes);
    }

    private static String key(JavaType.Variable v) {
        return v.getName() + "|" + v.getOwner();
    }
}
