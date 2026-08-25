package fix

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Failure, Success }

object OnFailureFix {
  def inSource(f: Future[Int], g: Future[Int]): Unit = {
    f.onComplete { case Failure(e: RuntimeException) => println(e.getMessage) }

    g.onComplete { case Success(v) => println(v) }
  }
}
