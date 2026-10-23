package com.acceldata.openrewrite.spark;

import org.openrewrite.Recipe;
import org.openrewrite.java.ChangeMethodName;

import java.util.Collections;
import java.util.List;

/**
 * Java-side counterpart to the Scalafix {@code UnionRewrite} rule: {@code
 * Dataset#unionAll} is deprecated in favor of {@code Dataset#union}, an exact
 * rename with identical semantics. Both the old and new methods are verified
 * present in the real {@code spark-sql_2.11-2.4.8.jar} (via {@code javap};
 * the Scala side is called through the same bytecode), so this rewrite is valid pre-migration too and
 * cannot break the pre-3.5.5 build.
 *
 * <p>Promoted to Tier 1: delegates to OpenRewrite's own
 * {@link ChangeMethodName} instead of a hand-rolled {@code
 * m.withName(m.getName().withSimpleName(...))} visitor -- the hand-rolled
 * form leaves the LST's method *type* still pointing at the old symbol,
 * harmless for a dry run but a real risk once recipes are composed and the
 * result is written to disk. {@code ChangeMethodName} keeps type attribution
 * correct, which matters in the Runner's fix mode.
 */
public class SparkUnionAllRename extends Recipe {

    @Override
    public String getDisplayName() {
        return "Rename Dataset#unionAll to Dataset#union";
    }

    @Override
    public String getDescription() {
        return "unionAll is an alias of union (not deprecated in 2.4 or 3.5.5); union is the name the docs use, with identical semantics.";
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
