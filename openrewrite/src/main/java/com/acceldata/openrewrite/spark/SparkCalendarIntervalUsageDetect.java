package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.marker.SearchResult;

/**
 * Java-side counterpart to the Scalafix {@code CalendarIntervalUsageDetect}
 * rule: date/timestamp subtraction returns {@code DayTimeIntervalType} from
 * Spark 3.2 onward, where 2.4 returned a type expressed via {@code
 * org.apache.spark.unsafe.types.CalendarInterval}. Same narrowing the Scala
 * rule uses: detecting the subtraction itself needs both operand types
 * resolved (declined project-wide, no reliable call-site type inference);
 * the code that actually breaks is code naming {@code CalendarInterval},
 * which is exactly detectable. Verified present in the real {@code
 * spark-unsafe_2.11-2.4.8.jar}.
 *
 * <p>Matches any type reference resolving to the class (a variable's
 * declared type, a cast, a method return type, an import) via {@code
 * J.Identifier}'s resolved type -- OpenRewrite represents an unqualified
 * type reference this way, the same as a qualified one's last segment.
 */
public class SparkCalendarIntervalUsageDetect extends Recipe {

    private static final String FQN = "org.apache.spark.unsafe.types.CalendarInterval";

    @Override
    public String getDisplayName() {
        return "Detect references to CalendarInterval";
    }

    @Override
    public String getDescription() {
        return "References to CalendarInterval -- from Spark 3.2 date/timestamp subtraction yields " +
                "DayTimeIntervalType instead, so code typed against CalendarInterval breaks.";
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
                if ("CalendarInterval".equals(id.getSimpleName()) && isCalendarInterval(id.getType())) {
                    return SearchResult.found(
                            id,
                            "CalendarInterval is referenced here. From Spark 3.2, subtracting two dates or two " +
                                    "timestamps produces DayTimeIntervalType, not CalendarIntervalType -- so code " +
                                    "that stores, casts or checks the result as CalendarInterval no longer type-checks."
                    );
                }
                return id;
            }

            private boolean isCalendarInterval(JavaType type) {
                return type instanceof JavaType.FullyQualified
                        && FQN.equals(((JavaType.FullyQualified) type).getFullyQualifiedName());
            }
        };
    }
}
