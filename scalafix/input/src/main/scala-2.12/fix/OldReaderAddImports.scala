/*
 rule=MigrateDeprecatedDataFrameReaderFuns
 */
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.{SparkSession, Dataset}

object BadReadsAddImports {
  def doMyWork(session: SparkSession, r: RDD[String], dataset: Dataset[String]) = {
    import session.implicits._
    val shouldRewriteBasic = session.read.json(r) // assert: MigrateDeprecatedDataFrameReaderFuns
    val r2 = session.sparkContext.parallelize(List("{}"))
    val shouldRewrite = session.read.json(r2) // assert: MigrateDeprecatedDataFrameReaderFuns
    val r3: RDD[String] = session.sparkContext.parallelize(List("{}"))
    val shouldRewriteExplicit = session.read.json(r3) // assert: MigrateDeprecatedDataFrameReaderFuns
    val noRewrite2 = session.read.json(dataset)
  }

  // The overwhelmingly common naming convention -- must resolve to "spark",
  // not a hardcoded "session" that doesn't exist in this scope.
  def doMoreWork(spark: SparkSession, r: RDD[String]) = {
    val shouldRewriteSpark = spark.read.json(r) // assert: MigrateDeprecatedDataFrameReaderFuns
  }
}
