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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Java-side counterpart to the Scalafix {@code SaveAsTablePathDetect} rule:
 * {@code saveAsTable} with a {@code path} option creates an external table at
 * that location, and from Spark 3.2 that CTAS fails when the location is a
 * non-empty directory, where 2.4 overwrote it -- the usual rerun after
 * dropping an external table, whose files stay.
 *
 * <p>Spark 3.5.5 checks only when the table does not exist and the mode is
 * ErrorIfExists ({@code CreateDataSourceTableAsSelectCommand} ->
 * {@code DataWritingCommand.assertEmptyRootPath}); Overwrite, Append, Ignore,
 * a Hive-format table and a DataSource V2 table (Delta, Iceberg) never reach
 * it. So this flags a {@code .option("path", ...)}, or an
 * {@code .options(...)} map built inline by a call with a {@code "path"} key
 * ({@code Map.of}, {@code ImmutableMap.of}, {@code Collections.singletonMap}),
 * on the writer chain, when the mode is absent (the default is ErrorIfExists),
 * an ErrorIfExists spelling, or not readable. A writer chain held in a
 * variable in the same file is followed once.
 *
 * <p>Feeds {@code spark.sql.legacy.allowNonEmptyLocationInCTAS} (Tier 2).
 */
public class SparkSaveAsTablePathDetect extends Recipe {

    private static final MethodMatcher SAVE_AS_TABLE =
            new MethodMatcher("org.apache.spark.sql.DataFrameWriter saveAsTable(..)");
    private static final List<String> ERROR_MODES = Arrays.asList("error", "errorifexists", "default");
    private static final List<String> UNAFFECTED_FORMATS = Arrays.asList("hive", "delta", "iceberg");

    @Override
    public String getDisplayName() {
        return "Detect saveAsTable with a path option in ErrorIfExists mode";
    }

    @Override
    public String getDescription() {
        return "saveAsTable with a `path` option in ErrorIfExists (default) mode; from Spark 3.2 it fails when the " +
                "location is not empty, where 2.4 overwrote it.";
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
                if (!SAVE_AS_TABLE.matches(m)) {
                    return m;
                }
                List<J.MethodInvocation> calls = new ArrayList<>();
                chain(m.getSelect(), true, calls);
                Boolean errorMode = errorIfExists(calls);
                if (calls.stream().anyMatch(this::setsPath) && !Boolean.FALSE.equals(errorMode) && !unaffectedFormat(calls)) {
                    return SearchResult.found(
                            m,
                            "saveAsTable with a `path` option, in " +
                                    (errorMode == null ? "a mode that may be ErrorIfExists" : "ErrorIfExists mode (the default)") +
                                    ". When the table does not exist but the directory holds files -- the usual rerun, " +
                                    "since dropping an external table keeps its files -- Spark 3.2+ fails the write " +
                                    "(non-empty CTAS location), where 2.4 overwrote the directory. The tool injects " +
                                    "spark.sql.legacy.allowNonEmptyLocationInCTAS=true to keep 2.4's behaviour."
                    );
                }
                return m;
            }

            /** The calls of the writer chain, down to {@code write()}, following a variable once. */
            private void chain(@Nullable Expression e, boolean followVariable, List<J.MethodInvocation> calls) {
                if (e instanceof J.MethodInvocation) {
                    J.MethodInvocation call = (J.MethodInvocation) e;
                    if (!"write".equals(call.getSimpleName())) {
                        calls.add(call);
                        chain(call.getSelect(), followVariable, calls);
                    }
                } else if (e instanceof J.Identifier && followVariable) {
                    chain(initializers.get(((J.Identifier) e).getSimpleName()), false, calls);
                }
            }

            private boolean setsPath(J.MethodInvocation call) {
                List<Expression> args = call.getArguments();
                if ("option".equals(call.getSimpleName()) && args.size() == 2) {
                    return isPathKey(args.get(0));
                }
                if ("options".equals(call.getSimpleName()) && args.size() == 1) {
                    Expression map = args.get(0);
                    if (map instanceof J.Identifier) {
                        map = initializers.get(((J.Identifier) map).getSimpleName());
                    }
                    if (map instanceof J.MethodInvocation) {
                        List<Expression> pairs = ((J.MethodInvocation) map).getArguments();
                        for (int i = 0; i < pairs.size(); i += 2) {
                            if (isPathKey(pairs.get(i))) {
                                return true;
                            }
                        }
                    }
                }
                return false;
            }

            /** TRUE for ErrorIfExists or no mode, FALSE for another mode, null when unreadable. */
            private @Nullable Boolean errorIfExists(List<J.MethodInvocation> calls) {
                for (J.MethodInvocation call : calls) {
                    if ("mode".equals(call.getSimpleName()) && call.getArguments().size() == 1) {
                        Expression arg = call.getArguments().get(0);
                        if (arg instanceof J.Literal && ((J.Literal) arg).getValue() instanceof String) {
                            return ERROR_MODES.contains(((String) ((J.Literal) arg).getValue()).toLowerCase());
                        }
                        if (arg instanceof J.FieldAccess) {
                            return "ErrorIfExists".equals(((J.FieldAccess) arg).getSimpleName());
                        }
                        if (arg instanceof J.Identifier) {
                            String name = ((J.Identifier) arg).getSimpleName();
                            return name.equals("ErrorIfExists") ? Boolean.TRUE : null;
                        }
                        return null;
                    }
                }
                return true;
            }

            private boolean unaffectedFormat(List<J.MethodInvocation> calls) {
                return calls.stream().anyMatch(call -> "format".equals(call.getSimpleName()) &&
                        call.getArguments().size() == 1 && call.getArguments().get(0) instanceof J.Literal &&
                        UNAFFECTED_FORMATS.contains(String.valueOf(((J.Literal) call.getArguments().get(0)).getValue()).toLowerCase()));
            }
        };
    }

    private static boolean isPathKey(Expression e) {
        return e instanceof J.Literal && "path".equalsIgnoreCase(String.valueOf(((J.Literal) e).getValue()));
    }
}
