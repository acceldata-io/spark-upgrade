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
    s.sql("""select second(ts) from t""") 
    // format_string's %0$: rewritten where the column name is discarded (aliased)...
    s.sql("""select format_string('%1$s', s) as f from t""") 
    // ...but an unaliased SELECT item would change its name: Tier 3 (.Manual), left as written.
    s.sql("select format_string('%0$s', s) from t") 
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
