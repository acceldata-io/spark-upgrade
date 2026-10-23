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
 * A purpose-built JVM runner for these recipes, invoked via {@code cs launch
 * -r ivy2Local <coordinate> -M com.acceldata.openrewrite.Runner -- ...}, the
 * same way {@code scalafix-cli} is invoked for the Scala rules.
 *
 * <p>Always dry-run: no write-to-disk API is ever called here. Every recipe
 * requested runs alone against the whole parsed source set, sequentially --
 * deliberately mirroring the Scala side's "one scalafixAll per Tier 1 rule,
 * sequentially, because two rules can collide on the same source span"
 * rationale, even though nothing is written back to disk (the {@code
 * --check}-equivalent).
 *
 * <p>Args: {@code --repo-root <path>} {@code --files-list <path>} (newline-
 * delimited absolute .java paths) {@code --classpath <path-separator-joined>}
 * {@code --recipes <comma-list|all>} {@code --output <jsonl-path>} {@code
 * --mode <analyze|fix>} (default {@code analyze}).
 *
 * <p>{@code analyze} mode is always dry-run: no write-to-disk API is ever
 * called, matching this doc's original contract exactly. {@code fix} mode
 * (for the Tier 1 recipes that rewrite, such as {@code SparkUnionAllRename}
 * and {@code SparkShuffleWriteMetricsRename})
 * additionally writes each recipe's result back to disk, the Java-side
 * equivalent of running {@code scalafixAll} without {@code --check}. Both modes emit the same JSONL findings
 * stream either way -- {@code fix} mode's caller reads it the same way
 * {@code analyze} mode's caller does,
 * so a Tier 1 rewrite that fires during codegen is still a reportable,
 * auditable finding, not a silent mutation.
 *
 * <p>Output is JSONL, one record per line: {@code {"type":"finding", ...}}
 * or {@code {"type":"warning", ...}} -- a per-file error is a warning
 * record, not an all-or-nothing exit code. Exit non-zero only for a
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
        boolean fixMode = "fix".equals(opts.getOrDefault("mode", "analyze"));

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

            // fix mode runs sequentially, each recipe's own output feeding
            // the next -- mirrors the Scala side's "one scalafixAll invocation
            // per Tier 1 rule, sequentially, because two rules can collide
            // on the same source span" rationale, now that this mode makes
            // real writes rather than only dry-run diffs.
            List<SourceFile> current = parsedOk;
            List<Recipe> recipes = RecipeCatalog.resolve(recipesArg);
            for (Recipe recipe : recipes) {
                String recipeId = recipe.getName() != null ? recipe.getName() : recipe.getDisplayName();
                LargeSourceSet sourceSet = new InMemoryLargeSourceSet(current);
                RecipeRun run = recipe.run(sourceSet, ctx);
                List<SourceFile> next = new ArrayList<>(current);
                for (Result result : run.getChangeset().getAllResults()) {
                    emitFindingsForResult(out, recipeId, result);
                    if (fixMode && result.getAfter() != null) {
                        writeResultToDisk(repoRoot, result);
                        replaceInPlace(next, result.getBefore(), (SourceFile) result.getAfter());
                    }
                }
                current = next;
            }
        }
    }

    private static void replaceInPlace(List<SourceFile> files, @org.openrewrite.internal.lang.Nullable SourceFile before, SourceFile after) {
        if (before == null) {
            return;
        }
        for (int i = 0; i < files.size(); i++) {
            if (files.get(i) == before) {
                files.set(i, after);
                return;
            }
        }
    }

    private static void writeResultToDisk(Path repoRoot, Result result) throws IOException {
        Path target = repoRoot.resolve(result.getAfter().getSourcePath());
        Files.createDirectories(target.getParent());
        Files.write(target, result.getAfter().printAll().getBytes(StandardCharsets.UTF_8));
    }

    private static void emitFindingsForResult(BufferedWriter out, String recipeId, Result result) throws IOException {
        SourceFile after = result.getAfter() != null ? result.getAfter() : result.getBefore();
        if (after == null) {
            return;
        }
        String file = after.getSourcePath().toString();
        String diff = result.diff();
        for (Hunk hunk : parseHunks(diff)) {
            writeFinding(out, recipeId, file, hunk.oldStartLine, hunk.snippet);
        }
    }

    /** One contiguous run of changed lines inside a unified-diff hunk --
     * NOT the whole hunk. See {@link #parseHunks}. Package-private so
     * {@code RunnerHunkParsingTest} can assert on it directly. */
    static final class Hunk {
        /** The line in the ORIGINAL file: findings are about the source the
         * analysis read, and a caller pairs them with diff hunks by old line. */
        final int oldStartLine;
        final String snippet;

        Hunk(int oldStartLine, String snippet) {
            this.oldStartLine = oldStartLine;
            this.snippet = snippet;
        }
    }

    // Standard unified-diff hunk header: "@@ -oldStart,oldLines +newStart,newLines @@[ trailer]".
    // Confirmed against a real spike that Result.diff() produces exactly this
    // shape, so a finding's lines come from the real diff rather than from a
    // label the source engine emits.
    //
    // Returns one entry per contiguous run of CHANGED lines, not one per hunk.
    // Both halves of that matter, and both were real bugs found by reading a
    // real analyze run's findings against the test project's own line numbers:
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
    // Line accounting is the standard unified-diff rule, on the OLD file's
    // side: context and `-` lines advance the old-file counter, `+` lines do
    // not (they exist only in the new file). Reporting new-file lines put
    // every finding after an added import a few lines low. A `\`
    // line ("\ No newline at end of file") is metadata and advances nothing.
    //
    // A block that only adds or removes imports is the side effect of a
    // rewrite elsewhere in the file, not a site of its own, so it is dropped
    // when the file has another block (otherwise an add_months rewrite is
    // reported three times, once per import it adds).
    static List<Hunk> parseHunks(String diff) {
        List<Hunk> all = parseBlocks(diff);
        List<Hunk> sites = new ArrayList<>();
        for (Hunk h : all) {
            if (!importsOnly(h.snippet)) {
                sites.add(h);
            }
        }
        return sites.isEmpty() ? all : sites;
    }

    private static boolean importsOnly(String snippet) {
        for (String line : snippet.split("\n")) {
            String code = line.length() > 0 ? line.substring(1).trim() : "";
            if (!code.isEmpty() && !code.startsWith("import ")) {
                return false;
            }
        }
        return true;
    }

    private static List<Hunk> parseBlocks(String diff) {
        List<Hunk> hunks = new ArrayList<>();
        String[] lines = diff.split("\n", -1);
        int i = 0;
        while (i < lines.length) {
            if (!lines[i].startsWith("@@")) {
                i++;
                continue;
            }
            int oldLine = parseOldStartLine(lines[i]);
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
                        blockStart = oldLine;
                    }
                    block.append(body).append('\n');
                    if (removed) {
                        oldLine++;
                    }
                } else {
                    if (block.length() > 0) {
                        hunks.add(new Hunk(blockStart, block.toString().trim()));
                        block.setLength(0);
                    }
                    oldLine++;
                }
            }
            if (block.length() > 0) {
                hunks.add(new Hunk(blockStart, block.toString().trim()));
            }
            i = j;
        }
        return hunks;
    }

    private static int parseOldStartLine(String hunkHeader) {
        int minusIdx = hunkHeader.indexOf('-');
        if (minusIdx < 0) {
            return 0;
        }
        int end = minusIdx + 1;
        while (end < hunkHeader.length() && (Character.isDigit(hunkHeader.charAt(end)))) {
            end++;
        }
        try {
            return Integer.parseInt(hunkHeader.substring(minusIdx + 1, end));
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
