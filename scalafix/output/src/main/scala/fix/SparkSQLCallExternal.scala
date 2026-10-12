import org.apache.spark._
import org.apache.spark.sql._

object OldQuery {
  def doQuery(s: SparkSession) {
    // Reported, not rewritten: the int(x) rewrite is the same expression.
    s.sql("""select
      cast(a as int),
      cast(b as int)
    from fart_tbl""") 
    // An audited fix is rewritten: 2.4 returned the integer second.
    s.sql("""select int(extract(second from ts)) from t""") 
    // We can't auto rewrite this :( easily.
    val q = "SELECT * FROM FARTS LIMIT 1"
    s.sql(q)
    // we should not change this
    fart("magic farts")
  }

  def fart(str: String) = {
    println(s"Fart ${str}")
  }
}
