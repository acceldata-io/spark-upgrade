/*
 rule=ScalaTestImportChange
 */
import org.scalatest.Matchers._ // assert: ScalaTestImportChange
import org.scalatest.FunSuite // assert: ScalaTestImportChange

class OldTest extends FunSuite { val a = 1 } // assert: ScalaTestImportChange
