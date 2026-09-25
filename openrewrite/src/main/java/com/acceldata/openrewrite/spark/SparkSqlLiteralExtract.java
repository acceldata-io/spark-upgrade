package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

/**
 * Not a migration-guide finding on its own -- a plumbing recipe that feeds
 * spark-migrate-cli's Java-side embedded-SQL (Family K) pipeline
 * ({@code JavaSqlLiteralAnalyzer.scala}), the Java counterpart to
 * {@code SparkSQLCallExternal}'s Scala-side sqlfluff bridge
 * (spark-migrate-code-review-2026-09-24.md SS5.2 named zero Family K
 * coverage for .java sources as the single largest remaining Java gap).
 *
 * <p>Rather than teach the openrewrite module to shell out to sqlfluff
 * itself (a new subprocess dependency this module has never needed) or add
 * a new JSONL record type to {@code Runner}'s wire contract, this recipe
 * reuses the existing SearchResult/diff/finding pipeline verbatim: it marks
 * the literal SQL argument with a {@code SearchResult} whose description
 * text IS the raw SQL, so {@code Runner}'s existing hunk-parsing already
 * gives the caller the exact file/line, and the SQL text rides along inside
 * the finding's {@code diff_snippet} as the {@code /*~~(<sql>)~~>*&#47;}
 * marker comment. {@code JavaSqlLiteralAnalyzer} un-wraps that marker and
 * feeds the recovered SQL to sqlfluff exactly as the Scala rule does.
 *
 * <p>Known, documented limitation (mirrors {@code SparkSQLCallExternal}'s
 * own honestly-recorded triple-quote edge case on the Scala side): a SQL
 * literal containing the literal substring {@code )~~>*&#47;} would
 * truncate the recovered text at that point. Accepted rather than built
 * around, for the same reason the Scala side accepts its own analogous
 * edge case -- vanishingly rare in real SQL text.
 *
 * <p>Only literal (non-interpolated/non-dynamic) String arguments are
 * matched, mirroring {@code SqlInStringDetect}'s own scope boundary on the
 * Scala side -- a non-literal argument has no static text to extract.
 */
public class SparkSqlLiteralExtract extends Recipe {

    private static final MethodMatcher SPARK_SESSION_SQL =
            new MethodMatcher("org.apache.spark.sql.SparkSession sql(java.lang.String)");
    private static final MethodMatcher SQL_CONTEXT_SQL =
            new MethodMatcher("org.apache.spark.sql.SQLContext sql(java.lang.String)");

    @Override
    public String getDisplayName() {
        return "Extract literal SQL passed to sql(...) for the Family K sqlfluff pipeline";
    }

    @Override
    public String getDescription() {
        return "Internal plumbing recipe: marks each literal SQL string passed to SparkSession#sql/SQLContext#sql " +
                "so spark-migrate-cli's Java-side embedded-SQL analyzer can recover the exact text and file/line.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if ((SPARK_SESSION_SQL.matches(m) || SQL_CONTEXT_SQL.matches(m)) && m.getArguments().size() == 1) {
                    J.MethodInvocation withMark = m;
                    if (m.getArguments().get(0) instanceof J.Literal
                            && ((J.Literal) m.getArguments().get(0)).getValue() instanceof String) {
                        String sql = (String) ((J.Literal) m.getArguments().get(0)).getValue();
                        J.Literal marked = SearchResult.found((J.Literal) m.getArguments().get(0), sql);
                        withMark = m.withArguments(java.util.Collections.singletonList(marked));
                    }
                    return withMark;
                }
                return m;
            }
        };
    }
}
