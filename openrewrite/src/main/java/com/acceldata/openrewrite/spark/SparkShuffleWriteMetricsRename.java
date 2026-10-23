package com.acceldata.openrewrite.spark;

import org.openrewrite.Recipe;
import org.openrewrite.java.ChangeMethodName;

import java.util.Arrays;
import java.util.List;

/**
 * Java-side counterpart to the Scalafix {@code ShuffleWriteMetricsRenameDetect}
 * rule: three deprecated {@code ShuffleWriteMetrics} accessors were removed in
 * Spark 3.0, each in favor of an exactly-equivalent shorter name. All six
 * method names (three old, three new) verified present in the real {@code
 * spark-core_2.11-2.4.8.jar} via {@code javap} -- same bytecode a
 * Java caller hits identically, so this rewrite is valid pre-migration too.
 *
 * <p>Promoted to Tier 1: delegates to three {@link
 * ChangeMethodName} instances instead of a hand-rolled visitor, for the same
 * type-attribution reason {@code SparkUnionAllRename} was retrofitted.
 */
public class SparkShuffleWriteMetricsRename extends Recipe {

    @Override
    public String getDisplayName() {
        return "Rename removed ShuffleWriteMetrics accessors";
    }

    @Override
    public String getDescription() {
        return "shuffleBytesWritten/shuffleWriteTime/shuffleRecordsWritten were removed in Spark 3.0; " +
                "renamed to bytesWritten/writeTime/recordsWritten.";
    }

    @Override
    public List<Recipe> getRecipeList() {
        return Arrays.asList(
                new ChangeMethodName(
                        "org.apache.spark.executor.ShuffleWriteMetrics shuffleBytesWritten()", "bytesWritten", false, false),
                new ChangeMethodName(
                        "org.apache.spark.executor.ShuffleWriteMetrics shuffleWriteTime()", "writeTime", false, false),
                new ChangeMethodName(
                        "org.apache.spark.executor.ShuffleWriteMetrics shuffleRecordsWritten()", "recordsWritten", false, false)
        );
    }
}
