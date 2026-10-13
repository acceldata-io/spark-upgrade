package com.acceldata.openrewrite;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins {@link Runner#parseHunks} against real {@code Result.diff()} output.
 *
 * <p>Both assertions here are regression tests for bugs found on 2026-09-24
 * by reading an actual {@code analyze} run's {@code findings.json} against
 * {@code fixtures/java-etl-spark2}'s own line numbers, not by reading code:
 * every Java finding's reported line was three lines early (the unified-diff
 * context size), and two independent occurrences sharing one hunk collapsed
 * into a single finding. Revert {@code parseHunks} to reporting the hunk
 * header's start once per hunk and the first two tests below go red.
 *
 * <p>The diffs are built with explicit {@code \n} joins rather than a text
 * block on purpose: a text block strips incidental leading whitespace, and
 * in a unified diff the leading space on a context line is load-bearing.
 */
class RunnerHunkParsingTest {

    /** Verbatim shape of the diff Result.diff() produced for the fixture's
     * SchemaBuilder.java, where array() is really on line 12 and map() on
     * line 16 -- two separate occurrences inside one hunk. */
    private static final String TWO_OCCURRENCES_ONE_HUNK = String.join("\n",
            "diff --git a/SchemaBuilder.java b/SchemaBuilder.java",
            "index 1111111..2222222 100644",
            "--- a/SchemaBuilder.java",
            "+++ b/SchemaBuilder.java",
            "@@ -9,11 +9,11 @@",
            " public class SchemaBuilder {",
            " ",
            "     public Column emptyTagsColumn() {",
            "-        return functions.array();",
            "+        return /*~~(array())~~>*/functions.array();",
            "     }",
            " ",
            "     public Column emptyAttributesColumn() {",
            "-        return functions.map();",
            "+        return /*~~(map())~~>*/functions.map();",
            "     }",
            " ",
            "     // Contrast case: a real argument is supplied -- must NOT be flagged.");

    /** The fixture's MetricsReporter.java: three consecutive changed lines,
     * really lines 10-12, which must stay ONE finding, not three. */
    private static final String ONE_CONTIGUOUS_BLOCK = String.join("\n",
            "@@ -7,9 +7,9 @@",
            " public class MetricsReporter {",
            " ",
            "     public String summarize(ShuffleWriteMetrics m) {",
            "-        return \"bytes=\" + m.shuffleBytesWritten()",
            "-                + \" time=\" + m.shuffleWriteTime()",
            "-                + \" records=\" + m.shuffleRecordsWritten();",
            "+        return \"bytes=\" + m.bytesWritten()",
            "+                + \" time=\" + m.writeTime()",
            "+                + \" records=\" + m.recordsWritten();",
            "     }",
            " ");

    @Test
    void reportsOneEntryPerOccurrenceAtTheLineThatActuallyChanged() {
        List<Runner.Hunk> hunks = Runner.parseHunks(TWO_OCCURRENCES_ONE_HUNK);

        assertThat(hunks).hasSize(2);
        assertThat(hunks.get(0).oldStartLine).isEqualTo(12);
        assertThat(hunks.get(0).snippet).contains("functions.array()").doesNotContain("functions.map()");
        assertThat(hunks.get(1).oldStartLine).isEqualTo(16);
        assertThat(hunks.get(1).snippet).contains("functions.map()").doesNotContain("functions.array()");
    }

    @Test
    void keepsAContiguousRunOfChangedLinesAsOneEntry() {
        List<Runner.Hunk> hunks = Runner.parseHunks(ONE_CONTIGUOUS_BLOCK);

        assertThat(hunks).hasSize(1);
        assertThat(hunks.get(0).oldStartLine).isEqualTo(10);
        assertThat(hunks.get(0).snippet).contains("shuffleBytesWritten").contains("recordsWritten");
    }

    /** A rewrite that adds imports: the call is on line 25 of the ORIGINAL
     * file. Reported at the old line, once -- not at 28 (the new file's line,
     * shifted by the two added imports), and not once more per import. */
    private static final String REWRITE_WITH_ADDED_IMPORTS = String.join("\n",
            "@@ -3,4 +3,6 @@",
            " import org.apache.spark.sql.Column;",
            "+import static org.apache.spark.sql.functions.last_day;",
            "+import static org.apache.spark.sql.functions.when;",
            " ",
            " public class ClaimEnricher {",
            "@@ -23,5 +25,5 @@",
            "     Dataset<Row> enrich(Dataset<Row> claims) {",
            "         return claims",
            "-            .withColumn(\"next_cycle\", add_months(col(\"service_date\"), 1));",
            "+            .withColumn(\"next_cycle\", when(to_date(col(\"service_date\")).equalTo(last_day(col(\"service_date\"))), last_day(add_months(col(\"service_date\"), 1))).otherwise(add_months(col(\"service_date\"), 1)));",
            "     }");

    @Test
    void reportsTheOriginalLineAndNotTheImportsARewriteAdded() {
        List<Runner.Hunk> hunks = Runner.parseHunks(REWRITE_WITH_ADDED_IMPORTS);

        assertThat(hunks).hasSize(1);
        assertThat(hunks.get(0).oldStartLine).isEqualTo(25);
        assertThat(hunks.get(0).snippet).contains("next_cycle");
    }

    @Test
    void anImportOnlyChangeIsStillReportedWhenItIsAllThereIs() {
        String importOnly = String.join("\n",
                "@@ -3,2 +3,2 @@",
                "-import org.apache.spark.ml.feature.OneHotEncoderEstimator;",
                "+import org.apache.spark.ml.feature.OneHotEncoder;",
                " ");
        List<Runner.Hunk> hunks = Runner.parseHunks(importOnly);
        assertThat(hunks).hasSize(1);
        assertThat(hunks.get(0).oldStartLine).isEqualTo(3);
    }

    @Test
    void aDiffWithNoHunkHeaderYieldsNothing() {
        assertThat(Runner.parseHunks("")).isEmpty();
        assertThat(Runner.parseHunks("diff --git a/X.java b/X.java\nindex 1..2 100644")).isEmpty();
    }
}
