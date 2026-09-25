/*
rule = SqlInStringDetect
 */
package fix

import org.apache.spark.sql.SparkSession

object SqlInStringDetectExample {
  def run(spark: SparkSession, table: String): Unit = {
    // Contrast case: a LITERAL argument is deliberately NOT flagged -- the
    // sqlfluff bridge and the Scala-regex Family K rules already inspect
    // literals directly and report their own, specific findings on this same
    // line (see the rule's own doc comment). This line used to carry an
    // `// assert: SqlInStringDetect`, left behind when the rule was narrowed;
    // it made the testkit permanently red and the failure was on record as a
    // version-coupled sqlfluff flake rather than what it is -- a stale
    // assertion. Removed 2026-09-24.
    spark.sql("SELECT * FROM people")
    spark.sql(s"SELECT * FROM $table") // assert: SqlInStringDetect
    spark.sql(buildQuery()) // assert: SqlInStringDetect
  }

  def buildQuery(): String = "SELECT 1"
}
