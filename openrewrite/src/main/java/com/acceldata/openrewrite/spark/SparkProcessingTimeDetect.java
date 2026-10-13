package com.acceldata.openrewrite.spark;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Preconditions;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeMethodName;
import org.openrewrite.java.ChangeMethodTargetToStatic;
import org.openrewrite.java.ChangeType;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaParser;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.TypeUtils;
import org.openrewrite.java.tree.TypedTree;
import org.openrewrite.marker.SearchResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Java-side counterpart to the Scalafix {@code MigrateTrigger} rule:
 * {@code org.apache.spark.sql.streaming.ProcessingTime} is removed in Spark
 * 3.x, and {@code Trigger.ProcessingTime(...)} replaces it.
 *
 * <p>What 2.4.8's {@code ProcessingTime} exposes ({@code javap}, 2026-10-13),
 * and where each goes. Every target overload exists in 3.5.5's {@code Trigger}
 * (and already in 2.4.8's, where it delegated to {@code ProcessingTime}):
 * <ul>
 *   <li>{@code apply(long)}, {@code new ProcessingTime(long)} &#8594;
 *       {@code Trigger.ProcessingTime(long)}</li>
 *   <li>{@code create(long, TimeUnit)} &#8594; {@code Trigger.ProcessingTime(long, TimeUnit)}</li>
 *   <li>{@code apply(String)}, {@code create(String)} &#8594; {@code Trigger.ProcessingTime(String)}</li>
 *   <li>{@code apply(scala.concurrent.duration.Duration)} &#8594;
 *       {@code Trigger.ProcessingTime(Duration)}</li>
 * </ul>
 * Then every remaining {@code ProcessingTime} type reference (a declared
 * variable, field, parameter or return type, a generic argument, the import)
 * becomes {@code Trigger}, the supertype every factory result already was.
 *
 * <p>Not retyped: a file that uses {@code instanceof ProcessingTime} or
 * {@code ProcessingTime.class}. As {@code Trigger} those would also match
 * {@code Trigger.Once()}/{@code Continuous(...)}, a silent behaviour change,
 * so the type stays and the 3.5.5 compile gate reports the file. A call to a
 * case-class instance method ({@code intervalMs()}, {@code copy(...)}) has no
 * {@code Trigger} equivalent and stops compiling either way.
 */
public class SparkProcessingTimeDetect extends Recipe {

    private static final String PROCESSING_TIME = "org.apache.spark.sql.streaming.ProcessingTime";
    private static final String TRIGGER = "org.apache.spark.sql.streaming.Trigger";

    private static final MethodMatcher CONSTRUCTOR = new MethodMatcher(PROCESSING_TIME + " <constructor>(long)");

    // Gives the template's output real type attribution without Spark on the
    // runner's own classpath.
    private static final String TRIGGER_STUB =
            "package org.apache.spark.sql.streaming;\n" +
            "public class Trigger {\n" +
            "    public static Trigger ProcessingTime(long intervalMs) { return null; }\n" +
            "}\n";

    @Override
    public String getDisplayName() {
        return "Rewrite the removed ProcessingTime trigger to Trigger.ProcessingTime";
    }

    @Override
    public String getDescription() {
        return "org.apache.spark.sql.streaming.ProcessingTime is removed in Spark 3.x. Each factory call and " +
                "constructor becomes the matching Trigger.ProcessingTime(...) overload, and ProcessingTime-typed " +
                "declarations become Trigger.";
    }

    /** {@code new ProcessingTime(ms)} &#8594; {@code Trigger.ProcessingTime(ms)}. Runs before the list below. */
    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaVisitor<ExecutionContext>() {
            @Override
            public J visitNewClass(J.NewClass newClass, ExecutionContext ctx) {
                J.NewClass n = (J.NewClass) super.visitNewClass(newClass, ctx);
                if (!CONSTRUCTOR.matches(n)) {
                    return n;
                }
                maybeRemoveImport(PROCESSING_TIME);
                maybeAddImport(TRIGGER);
                return JavaTemplate.builder("Trigger.ProcessingTime(#{any(long)})")
                        .imports(TRIGGER)
                        .javaParser(JavaParser.fromJavaVersion().dependsOn(TRIGGER_STUB))
                        .build()
                        .apply(getCursor(), n.getCoordinates().replace(), n.getArguments().get(0));
            }
        };
    }

    @Override
    public List<Recipe> getRecipeList() {
        List<Recipe> recipes = new ArrayList<>();
        // Every apply/create overload maps to the Trigger.ProcessingTime overload
        // with the same parameter list, so one pattern per name covers them all.
        for (String factory : new String[]{"apply", "create"}) {
            recipes.add(new ChangeMethodTargetToStatic(PROCESSING_TIME + " " + factory + "(..)", TRIGGER, TRIGGER, false));
            recipes.add(new ChangeMethodName(TRIGGER + " " + factory + "(..)", "ProcessingTime", false, false));
        }
        recipes.add(new RetypeToTrigger());
        return recipes;
    }

    /** {@link ChangeType} ProcessingTime &#8594; Trigger, skipped in a file that tests for the ProcessingTime type. */
    static class RetypeToTrigger extends Recipe {

        @Override
        public String getDisplayName() {
            return "Retype ProcessingTime references to Trigger";
        }

        @Override
        public String getDescription() {
            return "Changes ProcessingTime type references to Trigger, unless the file uses instanceof " +
                    "ProcessingTime or ProcessingTime.class.";
        }

        @Override
        public TreeVisitor<?, ExecutionContext> getVisitor() {
            return Preconditions.check(
                    Preconditions.not(new TypeTestOnProcessingTime()),
                    new ChangeType(PROCESSING_TIME, TRIGGER, false).getVisitor());
        }
    }

    private static class TypeTestOnProcessingTime extends JavaIsoVisitor<ExecutionContext> {
        @Override
        public J.InstanceOf visitInstanceOf(J.InstanceOf instanceOf, ExecutionContext ctx) {
            J.InstanceOf i = super.visitInstanceOf(instanceOf, ctx);
            boolean testsType = i.getClazz() instanceof TypedTree &&
                    TypeUtils.isOfClassType(((TypedTree) i.getClazz()).getType(), PROCESSING_TIME);
            return testsType ? SearchResult.found(i) : i;
        }

        @Override
        public J.FieldAccess visitFieldAccess(J.FieldAccess fieldAccess, ExecutionContext ctx) {
            J.FieldAccess f = super.visitFieldAccess(fieldAccess, ctx);
            boolean classLiteral = "class".equals(f.getSimpleName()) &&
                    TypeUtils.isOfClassType(f.getTarget().getType(), PROCESSING_TIME);
            return classLiteral ? SearchResult.found(f) : f;
        }
    }
}
