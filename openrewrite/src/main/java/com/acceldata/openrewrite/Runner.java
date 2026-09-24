package com.acceldata.openrewrite;

import org.openrewrite.ExecutionContext;
import org.openrewrite.InMemoryExecutionContext;
import org.openrewrite.LargeSourceSet;
import org.openrewrite.Recipe;
import org.openrewrite.RecipeRun;
import org.openrewrite.Result;
import org.openrewrite.SourceFile;
import org.openrewrite.internal.InMemoryLargeSourceSet;
import org.openrewrite.java.JavaParser;
import org.openrewrite.tree.ParseError;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The purpose-built JVM runner spark-migrate-java-openrewrite-integration-
 * 2026-09-18.md SS2.5/SS8.2 recommended (option 2) and the user's own
 * decision confirmed: invoked by spark-migrate-cli's {@code
 * OpenRewriteRunner} via {@code cs launch -r ivy2Local <coordinate> -M
 * com.acceldata.openrewrite.Runner -- ...}, mirroring exactly how {@code
 * CliScalafixRunner} invokes {@code scalafix-cli}.
 *
 * <p>Always dry-run: no write-to-disk API is ever called here. Every recipe
 * requested runs alone against the whole parsed source set, sequentially --
 * deliberately mirroring {@code PhaseARunner}'s "one scalafixAll per Tier 1
 * rule, sequentially, because two rules can collide on the same source span"
 * rationale, even though nothing is written back to disk (codegen wiring is
 * out of scope for this pass; this is Analysis's {@code --check}-equivalent
 * only).
 *
 * <p>Args: {@code --repo-root <path>} {@code --files-list <path>} (newline-
 * delimited absolute .java paths) {@code --classpath <path-separator-joined>}
 * {@code --recipes <comma-list|all>} {@code --output <jsonl-path>}.
 *
 * <p>Output is JSONL, one record per line: {@code {"type":"finding", ...}}
 * or {@code {"type":"warning", ...}} -- mirrors {@code
 * PySparklerOutputParser}'s parseError/transformerErrors split, not {@code
 * CliScalafixRunner}'s all-or-nothing exit code. Exit non-zero only for a
 * genuine runner-level failure (bad args, unreadable files-list, zero files
 * after filtering).
 */
public final class Runner {

    public static void main(String[] args) throws IOException {
        Map<String, String> opts = parseArgs(args);
        String repoRootArg = require(opts, "repo-root");
        String filesListArg = require(opts, "files-list");
        String classpathArg = opts.getOrDefault("classpath", "");
        String recipesArg = opts.getOrDefault("recipes", "all");
        String outputArg = require(opts, "output");

        Path repoRoot = Paths.get(repoRootArg).toAbsolutePath();
        Path outputPath = Paths.get(outputArg);
        Files.createDirectories(outputPath.toAbsolutePath().getParent());

        List<Path> files = readFilesList(Paths.get(filesListArg));
        if (files.isEmpty()) {
            System.err.println("[Runner] No files listed in " + filesListArg + " -- nothing to analyze");
            System.exit(1);
        }

        List<Path> classpath = splitClasspath(classpathArg);

        try (BufferedWriter out = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
            ExecutionContext ctx = new InMemoryExecutionContext(Throwable::printStackTrace);
            JavaParser parser = JavaParser.fromJavaVersion().classpath(classpath).logCompilationWarningsAndErrors(false).build();

            List<SourceFile> parsed = parser.parse(files, repoRoot, ctx).collect(Collectors.toList());

            List<SourceFile> parsedOk = new ArrayList<>();
            for (SourceFile sf : parsed) {
                if (sf instanceof ParseError) {
                    writeWarning(out, sf.getSourcePath().toString(), "could not parse this file cleanly; skipped for every recipe");
                } else {
                    parsedOk.add(sf);
                }
            }

            List<Recipe> recipes = RecipeCatalog.resolve(recipesArg);
            for (Recipe recipe : recipes) {
                String recipeId = recipe.getName() != null ? recipe.getName() : recipe.getDisplayName();
                LargeSourceSet sourceSet = new InMemoryLargeSourceSet(parsedOk);
                RecipeRun run = recipe.run(sourceSet, ctx);
                for (Result result : run.getChangeset().getAllResults()) {
                    emitFindingsForResult(out, recipeId, result);
                }
            }
        }
    }

    private static void emitFindingsForResult(BufferedWriter out, String recipeId, Result result) throws IOException {
        SourceFile after = result.getAfter() != null ? result.getAfter() : result.getBefore();
        if (after == null) {
            return;
        }
        String file = after.getSourcePath().toString();
        String diff = result.diff();
        for (Hunk hunk : parseHunks(diff)) {
            writeFinding(out, recipeId, file, hunk.newStartLine, hunk.snippet);
        }
    }

    /** One contiguous run of changed lines inside a unified-diff hunk --
     * NOT the whole hunk. See {@link #parseHunks}. Package-private so
     * {@code RunnerHunkParsingTest} can assert on it directly. */
    static final class Hunk {
        final int newStartLine;
        final String snippet;

        Hunk(int newStartLine, String snippet) {
            this.newStartLine = newStartLine;
            this.snippet = snippet;
        }
    }

    // Standard unified-diff hunk header: "@@ -oldStart,oldLines +newStart,newLines @@[ trailer]".
    // Confirmed against a real spike that Result.diff() produces exactly this
    // shape, hunk-parseable the same way PySparklerOutputParser already derives
    // its classification from a real diff rather than trusting a label the
    // source engine emits.
    //
    // Returns one entry per contiguous run of CHANGED lines, not one per hunk.
    // Both halves of that matter, and both were real bugs found by reading a
    // real analyze run's findings.json against the fixture's own line numbers
    // (2026-09-24):
    //
    //  1. The hunk header's `+newStart` is where the hunk BEGINS, which for a
    //     3-line-context diff is three lines above the first line that actually
    //     changed. Every Java finding's reported line was therefore three lines
    //     early -- e.g. SparkShuffleWriteMetricsRename reported
    //     MetricsReporter.java:7 (`public class MetricsReporter {`) for a change
    //     that is really on lines 10-12. A finding that points a reviewer at the
    //     wrong line is worse than useless in a tool whose whole contract is
    //     "auditable, human-reviewable findings".
    //  2. Two independent occurrences close enough to share one hunk collapsed
    //     into a single finding -- SchemaBuilder.java's `array()` (line 12) and
    //     `map()` (line 16) were reported once, as one finding at line 9. That
    //     is under-reporting, the same class of bug as reporting success for
    //     work not done.
    //
    // Line accounting is the standard unified-diff rule: context and `+` lines
    // advance the new-file counter, `-` lines do not (they exist only in the
    // old file). A `\` line ("\ No newline at end of file") is metadata and
    // advances nothing.
    static List<Hunk> parseHunks(String diff) {
        List<Hunk> hunks = new ArrayList<>();
        String[] lines = diff.split("\n", -1);
        int i = 0;
        while (i < lines.length) {
            if (!lines[i].startsWith("@@")) {
                i++;
                continue;
            }
            int newLine = parseNewStartLine(lines[i]);
            int blockStart = 0;
            StringBuilder block = new StringBuilder();

            int j = i + 1;
            for (; j < lines.length && !lines[j].startsWith("@@"); j++) {
                String body = lines[j];
                if (body.startsWith("\\")) {
                    continue;
                }
                boolean added = body.startsWith("+");
                boolean removed = body.startsWith("-");
                if (added || removed) {
                    if (block.length() == 0) {
                        blockStart = newLine;
                    }
                    block.append(body).append('\n');
                    if (added) {
                        newLine++;
                    }
                } else {
                    if (block.length() > 0) {
                        hunks.add(new Hunk(blockStart, block.toString().trim()));
                        block.setLength(0);
                    }
                    newLine++;
                }
            }
            if (block.length() > 0) {
                hunks.add(new Hunk(blockStart, block.toString().trim()));
            }
            i = j;
        }
        return hunks;
    }

    private static int parseNewStartLine(String hunkHeader) {
        int plusIdx = hunkHeader.indexOf('+');
        if (plusIdx < 0) {
            return 0;
        }
        int end = plusIdx + 1;
        while (end < hunkHeader.length() && (Character.isDigit(hunkHeader.charAt(end)))) {
            end++;
        }
        try {
            return Integer.parseInt(hunkHeader.substring(plusIdx + 1, end));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static void writeFinding(BufferedWriter out, String recipeId, String file, int line, String snippet) throws IOException {
        out.write("{\"type\":\"finding\",\"recipe_id\":" + json(recipeId) + ",\"file\":" + json(file) +
                ",\"line\":" + line + ",\"diff_snippet\":" + json(snippet) + "}");
        out.newLine();
    }

    private static void writeWarning(BufferedWriter out, String file, String message) throws IOException {
        out.write("{\"type\":\"warning\",\"file\":" + json(file) + ",\"message\":" + json(message) + "}");
        out.newLine();
    }

    /** Minimal, dependency-free JSON string encoder -- this module has no
     * general JSON library dependency, and the wire contract is deliberately
     * flat (one record per line), so a hand-rolled escaper is the same
     * "dependency-light on purpose" choice {@code FindingsSink} already makes
     * on the Scala side. */
    private static String json(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
        return sb.toString();
    }

    private static List<Path> readFilesList(Path filesListPath) throws IOException {
        List<String> lines = Files.readAllLines(filesListPath, StandardCharsets.UTF_8);
        List<Path> paths = new ArrayList<>();
        for (String line : lines) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) {
                paths.add(Paths.get(trimmed));
            }
        }
        return paths;
    }

    private static List<Path> splitClasspath(String classpathArg) {
        if (classpathArg == null || classpathArg.isEmpty()) {
            return List.of();
        }
        List<Path> paths = new ArrayList<>();
        for (String entry : classpathArg.split(java.io.File.pathSeparator)) {
            if (!entry.isEmpty()) {
                paths.add(Paths.get(entry));
            }
        }
        return paths;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.startsWith("--") && i + 1 < args.length) {
                opts.put(a.substring(2), args[i + 1]);
                i++;
            }
        }
        return opts;
    }

    private static String require(Map<String, String> opts, String key) {
        String v = opts.get(key);
        if (v == null) {
            System.err.println("[Runner] Missing required argument: --" + key);
            System.exit(1);
        }
        return v;
    }

    private Runner() {
    }
}
