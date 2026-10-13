/*
 rule = ScalaTestImportChange
 */
import org.scalatest.AsyncFunSuite // assert: ScalaTestImportChange

// ScalaTest 3.2 moved AsyncFunSuite to org.scalatest.funsuite.AsyncFunSuite
// (not the AsyncFunSuiteLike trait).
class OldAsyncTest extends AsyncFunSuite { val a = 1 }
