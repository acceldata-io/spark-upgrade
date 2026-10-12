package fix
import org.apache.spark.sql.{DataFrame, Dataset}

object UnionRewrite {
  def inSource(
    df1: DataFrame,
    df2: DataFrame,
    df3: DataFrame,
    ds1: Dataset[String],
    ds2: Dataset[String]
  ): Unit = {
    val res1 = df1.union(df2) 
    val res2a = df1.union(df2) 
    val res2 = res2a.union(df3) 
    val res3 = Seq(df1, df2, df3).reduce(_ union _) 
    val res4 = ds1.union(ds2) 
    val res5 = Seq(ds1, ds2).reduce(_ union _) 
  }

  // Not Spark: another library's unionAll keeps duplicates and its union
  // drops them, so renaming it would change the result.
  class Query(val rows: Seq[Int]) {
    def unionAll(o: Query): Query = new Query(rows ++ o.rows)
    def union(o: Query): Query = new Query((rows ++ o.rows).distinct)
  }
  def notSpark(a: Query, b: Query): Query = a.unionAll(b)
}
