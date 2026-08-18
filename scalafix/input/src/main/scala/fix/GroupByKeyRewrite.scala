/*
rule=GroupByKeyRewrite
 */
package fix
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GroupByKeyRewrite {
  def isSource1(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val ds1 =
      List("Paerson 1", "Person 2", "User 1", "User 2", "test", "gggg")
        .toDS()
        .groupByKey(l => l.substring(0, 3))
        .count()
        .withColumnRenamed("value", "newName") // assert: GroupByKeyRewrite

    val ds11 =
      List("Paerson 1", "Person 2", "User 1", "User 2", "test", "gggg")
        .toDS()
        .withColumnRenamed("value", "newName")

    val df11 =
      List("Paerson 1", "Person 2", "User 1", "User 2", "test", "gggg")
        .toDF()
        .withColumnRenamed("value", "newName")

    val ds2 =
      List("Paerson 1", "Person 2", "User 1", "User 2", "test", "gggg")
        .toDS()
        .groupByKey(l => l.substring(0, 3))
        .count()
        .select($"value", $"count(1)") // assert: GroupByKeyRewrite

    val ds3 =
      List("Paerson 1", "Person 2", "User 1", "User 2", "test", "gggg")
        .toDS()
        .groupByKey(l => l.substring(0, 3))
        .count()
        .select(col("value"), col("count(1)")) // assert: GroupByKeyRewrite

    val ds4 =
      List("Paerson 1", "Person 2", "User 1", "User 2", "test", "gggg")
        .toDS()
        .groupByKey(l => l.substring(0, 3))
        .count()
        .select('value, 'count (1)) // assert: GroupByKeyRewrite

    val ds5 =
      List("Paerson 1", "Person 2", "User 1", "User 2", "test", "gggg")
        .toDS()
        .groupByKey(l => l.substring(0, 3))
        .count()
        .withColumn("newNameCol", upper(col("value"))) // assert: GroupByKeyRewrite

    val ds6 =
      List("Paerson 1", "Person 2", "User 1", "User 2", "test", "gggg")
        .toDS()
        .groupByKey(l => l.substring(0, 3))
        .count()
        .withColumn("value", upper(col("value"))) // assert: GroupByKeyRewrite
  }
}
