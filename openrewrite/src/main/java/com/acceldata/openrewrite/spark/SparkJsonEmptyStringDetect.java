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

import java.util.List;

/**
 * Java-side counterpart to the Scalafix {@code JsonEmptyStringDetect} rule:
 * the JSON datasource no longer accepts an empty string as a value for a
 * non-String, non-Binary field from Spark 3.0 -- 2.4 read {@code ""} as
 * null, 3.0 treats the record as malformed. Narrowed like the Scala rule to
 * a {@code .json(...)} read whose fluent-chain receiver includes an explicit
 * {@code .schema(...)} call -- with an inferred schema every field that ever
 * holds {@code ""} is inferred as a string, so there is nothing to reject.
 *
 * <p>And, since 2026-10-15, to the modes where the malformed record is
 * visible. Probed on 2.4.8 and 3.5.5: in the default PERMISSIVE mode 3.5.5
 * gives 2.4's row (this field null, the others kept); FAILFAST fails the job,
 * DROPMALFORMED drops the row and a corrupt-record column fills. So the read
 * is flagged for those two modes, a {@code columnNameOfCorruptRecord} option
 * or a schema naming {@code _corrupt_record} (a DDL literal, or a schema built
 * elsewhere in a file that names it), a mode that
 * is not a literal, or an {@code options(...)} call the recipe cannot read.
 *
 * <p>Feeds {@code spark.sql.legacy.json.allowEmptyString.enabled}.
 */
public class SparkJsonEmptyStringDetect extends Recipe {

    private static final MethodMatcher JSON_MATCHER =
            new MethodMatcher("org.apache.spark.sql.DataFrameReader json(..)");
    private static final MethodMatcher SCHEMA_MATCHER =
            new MethodMatcher("org.apache.spark.sql.DataFrameReader schema(..)");
    private static final MethodMatcher OPTION_MATCHER =
            new MethodMatcher("org.apache.spark.sql.DataFrameReader option(String, ..)");
    private static final MethodMatcher OPTIONS_MATCHER =
            new MethodMatcher("org.apache.spark.sql.DataFrameReader options(..)");

    @Override
    public String getDisplayName() {
        return "Detect a JSON read with an explicit schema whose mode shows a malformed record";
    }

    @Override
    public String getDescription() {
        return "A JSON read with an explicit schema in FAILFAST/DROPMALFORMED mode or with a corrupt-record column; from " +
                "Spark 3.0 an empty string in a non-string field makes the record malformed instead of null.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            // A schema built elsewhere in the file (a StructType variable) is
            // read as naming the corrupt-record column if the file does.
            private boolean fileNamesCorrupt;

            @Override
            public J.CompilationUnit visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
                fileNamesCorrupt = cu.printAll().contains("\"_corrupt_record\"");
                return super.visitCompilationUnit(cu, ctx);
            }

            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (!JSON_MATCHER.matches(m) || m.getArguments().isEmpty() || m.getArguments().get(0) instanceof J.Empty) {
                    return m;
                }
                String effect = effect(m.getSelect());
                if (effect == null) {
                    return m;
                }
                return SearchResult.found(m,
                        "JSON read with an explicit schema: from Spark 3.0 an empty string in a non-string field makes " +
                                "the record malformed, which here " + effect + "; 2.4 read it as null.");
            }

            /** What the malformed record does on this chain, or null when
             * there is no explicit schema or the mode is PERMISSIVE. */
            private @Nullable String effect(@Nullable Expression receiver) {
                boolean schema = false;
                boolean corrupt = false;
                String mode = null;
                boolean unreadable = false;
                for (Expression e = receiver; e instanceof J.MethodInvocation; e = ((J.MethodInvocation) e).getSelect()) {
                    J.MethodInvocation call = (J.MethodInvocation) e;
                    List<Expression> args = call.getArguments();
                    if (SCHEMA_MATCHER.matches(call)) {
                        schema = true;
                        String ddl = literal(args.get(0));
                        corrupt |= ddl != null ? ddl.contains("_corrupt_record") : fileNamesCorrupt;
                    } else if (OPTION_MATCHER.matches(call) && args.size() == 2) {
                        String key = literal(args.get(0));
                        if ("mode".equalsIgnoreCase(key)) {
                            String value = literal(args.get(1));
                            if (value == null) {
                                unreadable = true;
                            } else {
                                mode = value;
                            }
                        } else if ("columnNameOfCorruptRecord".equalsIgnoreCase(key)) {
                            corrupt = true;
                        }
                    } else if (OPTIONS_MATCHER.matches(call)) {
                        unreadable = true;
                    }
                }
                if (!schema) {
                    return null;
                }
                if ("FAILFAST".equalsIgnoreCase(mode)) {
                    return "fails the job";
                }
                if ("DROPMALFORMED".equalsIgnoreCase(mode)) {
                    return "drops the whole row";
                }
                if (corrupt) {
                    return "fills the corrupt-record column";
                }
                if (unreadable && mode == null) {
                    return "fails or drops the row under FAILFAST/DROPMALFORMED (the mode here is not a literal)";
                }
                return null;
            }

            private @Nullable String literal(Expression e) {
                return e instanceof J.Literal && ((J.Literal) e).getValue() instanceof String
                        ? (String) ((J.Literal) e).getValue() : null;
            }
        };
    }
}
