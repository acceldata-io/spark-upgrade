package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

import java.util.Arrays;
import java.util.List;

/**
 * Java-side counterpart to the Scalafix {@code DateTimeFormatPatternValidator}
 * rule: from Spark 3.0, {@code to_date}/{@code to_timestamp}/{@code
 * unix_timestamp} parse and {@code date_format}/{@code from_unixtime} format
 * with {@code java.time.format.DateTimeFormatter} instead of {@code
 * java.text.SimpleDateFormat}. All five {@code (Column, String)} overloads
 * were confirmed present via {@code javap} against the real
 * {@code spark-sql_2.11-2.4.8.jar}.
 *
 * <p>It flags only a pattern {@link DateTimePatterns#check} finds 3.5.5 rejects or reads
 * differently -- the probed table the Scala and PySpark rules share. Feeds
 * {@code spark.sql.legacy.timeParserPolicy}.
 */
public class SparkDateTimeFormatPatternDetect extends Recipe {

    private static final List<MethodMatcher> PARSING = Arrays.asList(
            new MethodMatcher("org.apache.spark.sql.functions to_date(org.apache.spark.sql.Column, java.lang.String)"),
            new MethodMatcher("org.apache.spark.sql.functions to_timestamp(org.apache.spark.sql.Column, java.lang.String)"),
            new MethodMatcher("org.apache.spark.sql.functions unix_timestamp(org.apache.spark.sql.Column, java.lang.String)")
    );
    private static final List<MethodMatcher> FORMATTING = Arrays.asList(
            new MethodMatcher("org.apache.spark.sql.functions date_format(org.apache.spark.sql.Column, java.lang.String)"),
            new MethodMatcher("org.apache.spark.sql.functions from_unixtime(org.apache.spark.sql.Column, java.lang.String)")
    );

    @Override
    public String getDisplayName() {
        return "Detect a datetime format pattern Spark 3.x treats differently";
    }

    @Override
    public String getDescription() {
        return "A literal datetime pattern that Spark 3.0's DateTimeFormatter rejects or reads differently from 2.4's " +
                "SimpleDateFormat (see spark.sql.legacy.timeParserPolicy).";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                boolean parsing = PARSING.stream().anyMatch(p -> p.matches(m));
                if (!parsing && FORMATTING.stream().noneMatch(f -> f.matches(m))) {
                    return m;
                }
                Expression last = m.getArguments().get(m.getArguments().size() - 1);
                if (last instanceof J.Literal && ((J.Literal) last).getValue() instanceof String) {
                    String pattern = (String) ((J.Literal) last).getValue();
                    String reason = DateTimePatterns.check(pattern, parsing);
                    if (reason != null) {
                        return SearchResult.found(m, "Pattern \"" + pattern + "\" " + reason +
                                ". 2.4 used SimpleDateFormat; Spark 3.x uses DateTimeFormatter.");
                    }
                }
                return m;
            }
        };
    }
}
