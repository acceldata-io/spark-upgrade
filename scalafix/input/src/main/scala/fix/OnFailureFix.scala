/*
rule=onFailureFix
 */
package fix

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object OnFailureFix {
  def inSource(f: Future[Int], g: Future[Int]): Unit = {
    f.onFailure { // assert: onFailureFix
      case e: RuntimeException => println(e.getMessage)
    }

    g.onSuccess { // assert: onFailureFix
      case v => println(v)
    }
  }
}
