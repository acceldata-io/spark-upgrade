package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.marker.SearchResult;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Java-side port of the PySpark and Scalafix {@code CommandOutputSchemaDetect}:
 * {@code SHOW DATABASES}, {@code SHOW TABLES} and {@code DESCRIBE DATABASE}
 * name their output columns differently from Spark 3.0 ({@code databaseName}
 * and {@code database} -> {@code namespace},
 * {@code database_description_item}/{@code _value} -> {@code info_name}/{@code
 * info_value}), so code reading the result by the 2.4 name fails; positional
 * access is unaffected (probed on 3.5.5). Matched at the start of a literal
 * {@code spark.sql(...)}/{@code sqlContext.sql(...)} argument, or of the
 * first literal of a {@code +} concatenation.
 *
 * <p>Feeds {@code spark.sql.legacy.keepCommandOutputSchema} (a baseline
 * config), which restores 2.4's names for these commands only.
 */
public class SparkCommandOutputSchemaDetect extends Recipe {

    private static final MethodMatcher SESSION_SQL = new MethodMatcher("org.apache.spark.sql.SparkSession sql(String)");
    private static final MethodMatcher CONTEXT_SQL = new MethodMatcher("org.apache.spark.sql.SQLContext sql(String)");

    private static final Pattern COMMAND = Pattern.compile(
            "(?is)^\\s*(SHOW\\s+(?:DATABASES|SCHEMAS)|SHOW\\s+TABLES?\\s+EXTENDED|SHOW\\s+TABLES|" +
                    "DESC(?:RIBE)?\\s+(?:DATABASE|SCHEMA)(?:\\s+EXTENDED)?)\\b");

    @Override
    public String getDisplayName() {
        return "Detect a SQL command whose output columns Spark 3.0 renamed";
    }

    @Override
    public String getDescription() {
        return "SHOW DATABASES/SHOW TABLES/DESCRIBE DATABASE in embedded SQL; Spark 3.0 renamed their output columns.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (!(SESSION_SQL.matches(m) || CONTEXT_SQL.matches(m))) {
                    return m;
                }
                String sql = leadingLiteral(m.getArguments().get(0));
                Matcher found = sql == null ? null : COMMAND.matcher(sql);
                if (found == null || !found.find()) {
                    return m;
                }
                String command = String.join(" ", found.group(1).toUpperCase().split("\\s+"));
                String renamed = command.startsWith("SHOW DATABASES") || command.startsWith("SHOW SCHEMAS")
                        ? "databaseName -> namespace"
                        : command.startsWith("DESC")
                        ? "database_description_item/database_description_value -> info_name/info_value"
                        : "database -> namespace";
                return SearchResult.found(m, command + ": Spark 3.0 renamed its output columns (" + renamed + "). " +
                        "Code that reads the result by the 2.4 name fails; positional access is unaffected. " +
                        "spark.sql.legacy.keepCommandOutputSchema=true restores 2.4's names.");
            }

            private String leadingLiteral(Expression e) {
                if (e instanceof J.Literal && ((J.Literal) e).getValue() instanceof String) {
                    return (String) ((J.Literal) e).getValue();
                }
                if (e instanceof J.Binary && ((J.Binary) e).getOperator() == J.Binary.Type.Addition) {
                    return leadingLiteral(((J.Binary) e).getLeft());
                }
                return null;
            }
        };
    }
}
