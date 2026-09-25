package com.acceldata.openrewrite.spark;

import org.openrewrite.Recipe;
import org.openrewrite.java.ChangeMethodName;

import java.util.Collections;
import java.util.List;

/**
 * Java-side counterpart to the Scalafix {@code UnionRewrite} rule: {@code
 * Dataset#unionAll} is deprecated in favor of {@code Dataset#union}, an exact
 * rename with identical semantics. Both the old and new methods are verified
 * present in the real {@code spark-sql_2.11-2.4.8.jar} (via {@code javap},
 * 2026-09-21 -- COVERAGE.md Part 9 already recorded this for the Scala side;
 * the same bytecode is called identically from Java, so the fact transfers
 * without re-deriving it), so this rewrite is valid pre-migration too and
 * cannot break the pre-3.5.5 build.
 *
 * <p>Promoted to Tier 1 (2026-09-24 session): delegates to OpenRewrite's own
 * {@link ChangeMethodName} instead of a hand-rolled {@code
 * m.withName(m.getName().withSimpleName(...))} visitor -- the code review
 * that shipped the original four Java recipes (2026-09-24 SS4.6) flagged the
 * hand-rolled form as leaving the LST's method *type* still pointing at the
 * old symbol, harmless for a dry-run-only analyze pass but a real risk once
 * codegen composes recipes and actually writes the result to disk.
 * {@code ChangeMethodName} keeps type attribution correct, which matters now
 * that this recipe is wired into {@code codegen}'s fix mode.
 */
public class SparkUnionAllRename extends Recipe {

    @Override
    public String getDisplayName() {
        return "Rename Dataset#unionAll to Dataset#union";
    }

    @Override
    public String getDescription() {
        return "unionAll is deprecated; union is the exact, semantics-preserving equivalent.";
    }

    @Override
    public List<Recipe> getRecipeList() {
        return Collections.singletonList(
                new ChangeMethodName(
                        "org.apache.spark.sql.Dataset unionAll(org.apache.spark.sql.Dataset)",
                        "union",
                        false,
                        false
                )
        );
    }
}
