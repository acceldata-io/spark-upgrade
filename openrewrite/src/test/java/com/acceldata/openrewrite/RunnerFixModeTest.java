package com.acceldata.openrewrite;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end pin for {@code --mode fix} (added 2026-09-24 to wire codegen's
 * fix mode for the two Tier 1 Java recipes -- {@code SparkUnionAllRename}/
 * {@code SparkShuffleWriteMetricsRename}). Runs the real {@code Runner.main}
 * entry point, the same one {@code cs launch} invokes in production, against
 * a real temp file and the real test classpath -- not a unit test of
 * {@code RecipeCatalog} alone, because the bug class this guards against
 * (a recipe finding something to change but the write-to-disk step never
 * firing, or firing against the wrong path) only shows up at the {@code
 * main} level.
 */
class RunnerFixModeTest {

    @Test
    void fixModeRewritesUnionAllOnDisk(@TempDir Path tempDir) throws Exception {
        Path repoRoot = tempDir.resolve("repo");
        Files.createDirectories(repoRoot);
        Path javaFile = repoRoot.resolve("Combine.java");
        Files.write(javaFile, (
                "import org.apache.spark.sql.Dataset;\n" +
                "import org.apache.spark.sql.Row;\n" +
                "\n" +
                "class Combine {\n" +
                "    Dataset<Row> combine(Dataset<Row> a, Dataset<Row> b) {\n" +
                "        return a.unionAll(b);\n" +
                "    }\n" +
                "}\n"
        ).getBytes(StandardCharsets.UTF_8));

        Path filesList = tempDir.resolve("files.txt");
        Files.write(filesList, javaFile.toAbsolutePath().toString().getBytes(StandardCharsets.UTF_8));

        Path output = tempDir.resolve("out.jsonl");

        String classpath = System.getProperty("java.class.path");

        Runner.main(new String[]{
                "--repo-root", repoRoot.toAbsolutePath().toString(),
                "--files-list", filesList.toAbsolutePath().toString(),
                "--classpath", classpath,
                "--recipes", "com.acceldata.openrewrite.spark.SparkUnionAllRename",
                "--output", output.toAbsolutePath().toString(),
                "--mode", "fix"
        });

        String rewritten = new String(Files.readAllBytes(javaFile), StandardCharsets.UTF_8);
        assertThat(rewritten).contains("a.union(b)");
        assertThat(rewritten).doesNotContain("unionAll");

        List<String> findingLines = Files.readAllLines(output);
        assertThat(findingLines).anySatisfy(line -> assertThat(line).contains("\"type\":\"finding\""));
    }

    @Test
    void analyzeModeNeverWritesToDisk(@TempDir Path tempDir) throws Exception {
        Path repoRoot = tempDir.resolve("repo");
        Files.createDirectories(repoRoot);
        Path javaFile = repoRoot.resolve("Combine.java");
        String original =
                "import org.apache.spark.sql.Dataset;\n" +
                "import org.apache.spark.sql.Row;\n" +
                "\n" +
                "class Combine {\n" +
                "    Dataset<Row> combine(Dataset<Row> a, Dataset<Row> b) {\n" +
                "        return a.unionAll(b);\n" +
                "    }\n" +
                "}\n";
        Files.write(javaFile, original.getBytes(StandardCharsets.UTF_8));

        Path filesList = tempDir.resolve("files.txt");
        Files.write(filesList, javaFile.toAbsolutePath().toString().getBytes(StandardCharsets.UTF_8));

        Path output = tempDir.resolve("out.jsonl");
        String classpath = System.getProperty("java.class.path");

        Runner.main(new String[]{
                "--repo-root", repoRoot.toAbsolutePath().toString(),
                "--files-list", filesList.toAbsolutePath().toString(),
                "--classpath", classpath,
                "--recipes", "com.acceldata.openrewrite.spark.SparkUnionAllRename",
                "--output", output.toAbsolutePath().toString()
        });

        String unchanged = new String(Files.readAllBytes(javaFile), StandardCharsets.UTF_8);
        assertThat(unchanged).isEqualTo(original);
    }
}
