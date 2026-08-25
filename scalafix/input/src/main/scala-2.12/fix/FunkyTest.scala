/*
 rules = [ScalaTestImportChange, ScalaTestExtendsFix]
 */
import org.scalatest.Matchers._ // assert: ScalaTestImportChange
import org.scalatest.FunSuite // assert: ScalaTestImportChange

class OldTest extends FunSuite { val a = 1 } // assert: ScalaTestExtendsFix

// A grouped import -- the old whole-statement quasiquote required exactly
// one importee and silently missed this shape entirely.
import org.scalatest.{FunSuiteLike, Suite} // assert: ScalaTestImportChange

class OldTest3 extends FunSuiteLike with Suite { val b = 2 } // assert: ScalaTestExtendsFix
