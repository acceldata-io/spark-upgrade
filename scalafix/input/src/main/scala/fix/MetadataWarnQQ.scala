/*
rule=MetadataWarnQQ
 */
package fix

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.Metadata

object MetadataWarnQQ {
  // At-risk: 1-arg alias, no explicit metadata -- Spark 3.0 will propagate
  // the underlying expression's metadata here instead of freezing it.
  def renamesWithoutMetadata(df: DataFrame): DataFrame =
    df.select(
      col("id"),
      col("v").as("newV") // assert: MetadataWarnQQ
    )

  def renamesWithName(df: DataFrame): DataFrame =
    df.select(col("v").name("newV")) // assert: MetadataWarnQQ

  // Not at risk: the 2-arg escape hatch already pins the metadata explicitly.
  def renamesWithMetadata(df: DataFrame): DataFrame =
    df.select(
      col("id"),
      col("v").as(
        "newV",
        Metadata.fromJson(
          """{"desc": "replace old V"}"""
        )
      )
    )

  // Not at risk: Dataset.as sets a join alias, unrelated to Column metadata.
  def aliasesDataset(df: DataFrame): DataFrame = df.as("d")

  // Not at risk: identifiers merely named "select"/"as" with no Spark Column
  // call -- the previous version of this rule flagged this file.
  val select = 1
  val as = 2
  def sum: Int = select + as
}
