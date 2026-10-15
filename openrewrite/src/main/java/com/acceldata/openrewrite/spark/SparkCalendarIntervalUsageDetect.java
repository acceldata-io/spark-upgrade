package com.acceldata.openrewrite.spark;

import org.openrewrite.Cursor;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code CalendarIntervalUsageDetect}
 * rule: Spark 3.0 changed {@code org.apache.spark.unsafe.types.CalendarInterval}
 * (a {@code days} field added; the two-argument constructor, the
 * {@code MICROS_PER_*} constants, the {@code from*String} parsers,
 * {@code milliseconds()}, {@code add}, {@code subtract} and {@code negate}
 * removed), so the code that breaks is code naming {@code CalendarInterval},
 * which is exactly detectable. Verified present in the real {@code
 * spark-unsafe_2.11-2.4.8.jar}.
 *
 * <p>Matches any type reference resolving to the class (a variable's
 * declared type, a cast, a method return type, an import) via {@code
 * J.Identifier}'s resolved type -- OpenRewrite represents an unqualified
 * type reference this way, the same as a qualified one's last segment.
 *
 * <p>Skips the class name of a {@code new CalendarInterval(months,
 * microseconds)} that {@link SparkCalendarIntervalConstructorRewrite} rewrites.
 */
public class SparkCalendarIntervalUsageDetect extends Recipe {

    private static final String FQN = "org.apache.spark.unsafe.types.CalendarInterval";

    @Override
    public String getDisplayName() {
        return "Detect references to CalendarInterval";
    }

    @Override
    public String getDescription() {
        return "References to CalendarInterval -- Spark 3.0 added a days field and removed the two-argument " +
                "constructor, the MICROS_PER_* constants, the from*String parsers, milliseconds(), add, subtract and negate.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.Import visitImport(J.Import anImport, ExecutionContext ctx) {
                // Deliberately not visited: an import's qualified-name segments
                // are represented as plain J.Identifiers too, unlike scalameta's
                // separate Importee node, so without this override the import
                // line itself would falsely match -- something the Scala rule
                // (which only matches Type.Name/Term.Name, never an Importee)
                // never does. Marking an import line achieves nothing actionable
                // anyway; the real signal is a type reference in code.
                return anImport;
            }

            @Override
            public J.Identifier visitIdentifier(J.Identifier identifier, ExecutionContext ctx) {
                J.Identifier id = super.visitIdentifier(identifier, ctx);
                if ("CalendarInterval".equals(id.getSimpleName()) && isCalendarInterval(id.getType()) &&
                        !namesRewrittenConstructor()) {
                    return SearchResult.found(
                            id,
                            "CalendarInterval is referenced here. Spark 3.0 added a days field and removed the " +
                                    "(months, microseconds) constructor, the MICROS_PER_* constants, the from*String " +
                                    "parsers, milliseconds(), add, subtract and negate: code calling those no longer " +
                                    "compiles, and code reading microseconds silently gets different numbers."
                    );
                }
                return id;
            }

            /** This identifier is the class name in a constructor call the rewrite recipe handles. */
            private boolean namesRewrittenConstructor() {
                Cursor parent = getCursor().getParentTreeCursor();
                if (parent.getValue() instanceof J.FieldAccess) {
                    parent = parent.getParentTreeCursor();
                }
                return parent.getValue() instanceof J.NewClass &&
                        SparkCalendarIntervalConstructorRewrite.isRewritable((J.NewClass) parent.getValue(), parent);
            }

            private boolean isCalendarInterval(JavaType type) {
                return type instanceof JavaType.FullyQualified
                        && FQN.equals(((JavaType.FullyQualified) type).getFullyQualifiedName());
            }
        };
    }
}
