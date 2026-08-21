/*
 rule=MigrateHiveContext
 */
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.hive.{HiveContext => HiveCtx} // assert: MigrateHiveContext

object BadHiveContextMagic2 {
  def hiveContextFunc(sc: SparkContext): HiveCtx = { // assert: MigrateHiveContext
    val hiveContext1 = new HiveCtx(sc) // assert: MigrateHiveContext
    import hiveContext1.implicits._
    hiveContext1
  }
}
