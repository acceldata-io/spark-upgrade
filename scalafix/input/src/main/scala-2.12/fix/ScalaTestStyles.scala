/*
 rules = [ScalaTestImportChange, ScalaTestExtendsFix]
 */
package scalateststyles

import org.scalatest.FlatSpec // assert: ScalaTestImportChange
import org.scalatest.{WordSpec, BeforeAndAfter} // assert: ScalaTestImportChange
import org.scalatest.AsyncFlatSpec // assert: ScalaTestImportChange

// ScalaTest 3.2 removed every 3.0 suite style from org.scalatest, not only FunSuite.
class FlatTest extends FlatSpec { val a = 1 } // assert: ScalaTestExtendsFix

class WordTest extends WordSpec with BeforeAndAfter { val b = 2 } // assert: ScalaTestExtendsFix

// The Async styles keep their names; only the package moves.
class AsyncTest extends AsyncFlatSpec { val c = 3 }
