/*
rule = SparkSqlExtract
 */
package fix

import org.apache.spark.sql.SparkSession

object SparkSqlExtractExample {
  val Orders = "orders"
  val Daily = """SELECT d, count(*) FROM orders GROUP BY d"""
  val Margin = """SELECT a
                 |FROM t""".stripMargin

  def run(spark: SparkSession, table: String, day: String, n: Int, built: StringBuilder): Unit = {
    spark.sql("SELECT 1") /* assert: SparkSqlExtract
              ^^^^^^^^^^
literal
Change: SELECT 1
*/
    spark.sql(Margin) /* assert: SparkSqlExtract
              ^^^^^^
literal
Change: SELECT a
FROM t
*/
    spark.sql(Daily) /* assert: SparkSqlExtract
              ^^^^^
literal
Change: SELECT d, count(*) FROM orders GROUP BY d
*/
    spark.sql(s"SELECT * FROM $table WHERE d = '$day'") /* assert: SparkSqlExtract
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
template 2
Change: SELECT * FROM __param__ WHERE d = '__param__'
*/
    spark.sql(s"SELECT * FROM $Orders LIMIT ${10}") /* assert: SparkSqlExtract
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
literal
Change: SELECT * FROM orders LIMIT 10
*/
    spark.sql(f"SELECT round(x, 2) FROM t WHERE x > $n%d AND p LIKE 'a%%'") /* assert: SparkSqlExtract
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
template 1
Change: SELECT round(x, 2) FROM t WHERE x > 0 AND p LIKE 'a%'
*/
    spark.sql("SELECT count(t.*) FROM " + table + " t") /* assert: SparkSqlExtract
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
template 1
Change: SELECT count(t.*) FROM __param__ t
*/
    spark.sql("SELECT * FROM %s WHERE d = '%s'".format(table, day)) /* assert: SparkSqlExtract
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
template 2
Change: SELECT * FROM __param__ WHERE d = '__param__'
*/
    spark.sql(table) /* assert: SparkSqlExtract
              ^^^^^
dynamic
Change:
*/
    spark.sql(built.toString) /* assert: SparkSqlExtract
              ^^^^^^^^^^^^^^
dynamic
Change:
*/
  }
}

object SparkSqlExtractCollections {
  val Ddl = Seq(
    "CREATE TABLE a (c CHAR(2))", /* assert: SparkSqlExtract
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
literal
Change: CREATE TABLE a (c CHAR(2))
*/
    """DROP TABLE b""") /* assert: SparkSqlExtract
    ^^^^^^^^^^^^^^^^^^
literal
Change: DROP TABLE b
*/

  def run(spark: SparkSession, built: Seq[String]): Unit = {
    Ddl.foreach(spark.sql)
    List("SELECT 2").map(q => spark.sql(q)) /* assert: SparkSqlExtract
         ^^^^^^^^^^
literal
Change: SELECT 2
*/
    built.foreach(spark.sql(_)) /* assert: SparkSqlExtract
                  ^^^^^^^^^^^^
dynamic
Change:
*/
  }
}
