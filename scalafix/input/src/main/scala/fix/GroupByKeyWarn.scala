/*
rule=GroupByKeyWarn
 */
package fix
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions.{col, sum}

class GroupByKeyWarn {
  def inSource(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val ds1 = List("Person 1", "User 1", "Test")
      .toDS()
      .groupByKey(l => l.substring(0, 3))
      .count() // assert: GroupByKeyWarn

    val noChange: Dataset[String] = List("1").toDS()
    val noChangeMore = List("1").toDS().count()

    // The grouped dataset held in a val first.
    val grouped = noChange.groupByKey(l => l.length)
    val counted = grouped.count() // assert: GroupByKeyWarn

    val aggregated = List(1, 2, 3).toDS()
      .groupByKey(l => l > 1)
      .agg(sum(col("value")).as[Long]) // assert: GroupByKeyWarn

    // mapValues keeps the grouping, so count() after it still outputs the key.
    val viaMapValues = List(1, 2, 3).toDS()
      .groupByKey(l => l % 2)
      .mapValues(_ * 2)
      .count() // assert: GroupByKeyWarn

    val g = List(1, 2, 3).toDS().groupByKey(l => l % 2)
    val reduced = g.reduceGroups(_ + _) // assert: GroupByKeyWarn

    // These never output the grouping column: not flagged.
    val mapped = g.mapGroups((k, vs) => k + vs.size)
    val flatMapped = g.flatMapGroups((k, vs) => vs)
    val keys = g.keys

    // Untyped groupBy and Dataset.count are unrelated.
    val ds6 = Seq("test 1", "test 2", "test 3").toDS().groupBy("value").count()
    val ds7 = Seq("test 1", "test 2", "test 3").toDF().groupBy("value").agg(sum("value"))
  }

}
