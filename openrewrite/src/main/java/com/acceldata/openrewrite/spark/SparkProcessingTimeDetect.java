package com.acceldata.openrewrite.spark;

import org.openrewrite.Recipe;
import org.openrewrite.java.ChangeMethodName;
import org.openrewrite.java.ChangeMethodTargetToStatic;

import java.util.Arrays;
import java.util.List;

/**
 * Java-side counterpart to the Scalafix {@code MigrateTrigger} rule:
 * {@code org.apache.spark.sql.streaming.ProcessingTime} is removed in favor
 * of {@code Trigger.ProcessingTime(...)}/{@code Trigger.Once()}/{@code
 * Trigger.Continuous(...)}. Scala emits static forwarder methods for a
 * case-class companion object with no naming clash, so {@code
 * ProcessingTime.apply(long)}/{@code ProcessingTime.create(long, TimeUnit)}
 * are genuinely callable from Java exactly like any other static method --
 * confirmed via {@code javap} against the real {@code
 * spark-sql_2.11-2.4.8.jar} (2026-09-24), where both show up as ordinary
 * {@code public static} methods on the {@code ProcessingTime} class itself,
 * not only on its Scala-only {@code MODULE$} singleton.
 *
 * <p>Promoted to Tier 1 (2026-09-29 coverage review): both the removed
 * factory's return type ({@code Trigger}, since {@code ProcessingTime}
 * itself implemented that interface) and the replacement's argument shape
 * are identical, so this composes to an exact, semantics-preserving
 * rewrite rather than a structural one -- unlike {@code
 * SparkSqlContextConstructorDetect}/{@code SparkHiveContextConstructorDetect},
 * where the replacement changes the shape of the whole call (a constructor
 * becomes a multi-step builder chain) and the resulting variable's type
 * changes too, so those stay Tier 3 (see their own doc comments). Here the
 * call site's own type never changes -- only which class holds the static
 * method and what it's named -- so {@link ChangeMethodTargetToStatic}
 * (retarget {@code ProcessingTime} &#8594; {@code Trigger}, same method name)
 * composed with {@link ChangeMethodName} (rename {@code apply}/{@code create}
 * &#8594; {@code ProcessingTime}) expresses it exactly, the same
 * getRecipeList()-composition discipline {@code SparkUnionAllRename} already
 * established for a single-step rename. Verified end to end via
 * {@code SparkProcessingTimeDetectTest} before promoting, not assumed from
 * the API alone.
 */
public class SparkProcessingTimeDetect extends Recipe {

    @Override
    public String getDisplayName() {
        return "Rewrite the removed ProcessingTime factory to Trigger.ProcessingTime";
    }

    @Override
    public String getDescription() {
        return "org.apache.spark.sql.streaming.ProcessingTime is removed in Spark 3.x; " +
                "Trigger.ProcessingTime(...) is the exact, semantics-preserving replacement " +
                "(same argument shape).";
    }

    @Override
    public List<Recipe> getRecipeList() {
        return Arrays.asList(
                new ChangeMethodTargetToStatic(
                        "org.apache.spark.sql.streaming.ProcessingTime apply(long)",
                        "org.apache.spark.sql.streaming.Trigger",
                        "org.apache.spark.sql.streaming.Trigger",
                        false
                ),
                new ChangeMethodName(
                        "org.apache.spark.sql.streaming.Trigger apply(long)",
                        "ProcessingTime",
                        false,
                        false
                ),
                new ChangeMethodTargetToStatic(
                        "org.apache.spark.sql.streaming.ProcessingTime create(long, java.util.concurrent.TimeUnit)",
                        "org.apache.spark.sql.streaming.Trigger",
                        "org.apache.spark.sql.streaming.Trigger",
                        false
                ),
                new ChangeMethodName(
                        "org.apache.spark.sql.streaming.Trigger create(long, java.util.concurrent.TimeUnit)",
                        "ProcessingTime",
                        false,
                        false
                )
        );
    }
}
