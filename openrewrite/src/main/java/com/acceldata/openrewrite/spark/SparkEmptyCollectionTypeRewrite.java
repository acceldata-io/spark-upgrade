package com.acceldata.openrewrite.spark;

import org.openrewrite.Cursor;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaParser;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;

/**
 * Keeps Spark 2.4's type for a no-argument {@code functions.array()} /
 * {@code functions.map()}: 2.4 typed them {@code array<string>} /
 * {@code map<string,string>}, 3.0+ types them {@code array<void>} /
 * {@code map<void,void>}. 2.4's empty collections are also non-null inside
 * ({@code containsNull=false}, {@code valueContainsNull=false}), so the
 * rewrite casts to exactly that type:
 * <pre>
 *   array()  &#8594;  array().cast(DataTypes.createArrayType(DataTypes.StringType, false))
 *   map()    &#8594;  map().cast(DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, false))
 * </pre>
 * On 3.5.5 that gives 2.4.8's full schema, nullability included, alone and
 * inside {@code coalesce}, {@code concat} and {@code when/otherwise}, and the
 * same Parquet schema (probed). {@code
 * .cast("array<string>")} would not: it gives {@code containsNull=true}.
 *
 * <p>Applied only where the generated column name is discarded ({@link
 * ColumnNames}). The same probe showed 3.5.5 still names the cast column
 * {@code array()}, but the rewrite does not rely on that. Every other call is
 * left to {@link SparkEmptyCollectionTypeDetect}, which feeds the legacy config.
 * A call that is already the receiver of {@code .cast(...)} is typed by the
 * user and needs neither.
 */
public class SparkEmptyCollectionTypeRewrite extends Recipe {

    private static final String FUNCTIONS = "org.apache.spark.sql.functions";
    private static final String COLUMN = "org.apache.spark.sql.Column";

    private static final MethodMatcher ARRAY = new MethodMatcher(FUNCTIONS + " array(" + COLUMN + "...)");
    private static final MethodMatcher MAP = new MethodMatcher(FUNCTIONS + " map(" + COLUMN + "...)");
    private static final MethodMatcher CAST = new MethodMatcher(COLUMN + " cast(..)");

    private static final String DATA_TYPES = "org.apache.spark.sql.types.DataTypes";

    // Real type attribution for the template output without Spark on the
    // runner's own classpath.
    private static final String[] STUBS = {
            "package org.apache.spark.sql.types;\n" +
            "public abstract class DataType {}\n",
            "package org.apache.spark.sql.types;\n" +
            "public class DataTypes {\n" +
            "    public static final DataType StringType = null;\n" +
            "    public static DataType createArrayType(DataType elementType, boolean containsNull) { return null; }\n" +
            "    public static DataType createMapType(DataType keyType, DataType valueType, boolean valueContainsNull) { return null; }\n" +
            "}\n",
            "package org.apache.spark.sql;\n" +
            "public class Column {\n" +
            "    public Column cast(org.apache.spark.sql.types.DataType to) { return null; }\n" +
            "}\n"
    };

    @Override
    public String getDisplayName() {
        return "Keep Spark 2.4's type for no-argument array()/map()";
    }

    @Override
    public String getDescription() {
        return "From Spark 3.0, array() and map() with no arguments are typed array<void>/map<void,void> instead of " +
                "2.4's non-null array<string>/map<string,string>. Casts the call to exactly that type.";
    }

    /** A no-argument {@code array()}/{@code map()} that is not already the receiver of {@code .cast(...)}. */
    static boolean isUntypedEmptyCollection(J.MethodInvocation m, Cursor cursor) {
        boolean noArguments = m.getArguments().size() == 1 && m.getArguments().get(0) instanceof J.Empty;
        if (!noArguments || !(ARRAY.matches(m) || MAP.matches(m))) {
            return false;
        }
        Object parent = cursor.getParentTreeCursor().getValue();
        return !(parent instanceof J.MethodInvocation && CAST.matches((J.MethodInvocation) parent) &&
                ((J.MethodInvocation) parent).getSelect() != null &&
                ((J.MethodInvocation) parent).getSelect().getId().equals(m.getId()));
    }

    /** True when the call (unvisited, at {@code cursor}) is one this recipe rewrites. */
    static boolean isRewritable(J.MethodInvocation m, Cursor cursor) {
        return isUntypedEmptyCollection(m, cursor) && ColumnNames.isDiscarded(m, cursor);
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                boolean rewrite = isRewritable(method, getCursor());
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);
                if (!rewrite) {
                    return m;
                }
                String type = ARRAY.matches(m)
                        ? "DataTypes.createArrayType(DataTypes.StringType, false)"
                        : "DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, false)";
                maybeAddImport(DATA_TYPES);
                return JavaTemplate.builder("#{any(org.apache.spark.sql.Column)}.cast(" + type + ")")
                        .imports(DATA_TYPES)
                        .javaParser(JavaParser.fromJavaVersion().dependsOn(STUBS))
                        .build()
                        .apply(getCursor(), m.getCoordinates().replace(), m);
            }
        };
    }
}
