/*
 rule=ScalaTestExtendsFix
 */
// Own package so these local look-alike names don't leak into other
// fixtures' root-package scope and shadow their real imports.
package scalatestextendsfixlocalcontrast

trait Farts {
}

// Not at risk: the declaration itself must not be renamed -- only a
// supertype REFERENCE (inside an extends/with clause) should be. The old
// rule's bare, positionless Type.Name match renamed this declaration too.
trait FunSuite {
}

// So that the rewritten "extends AnyFunSuite" below still resolves --
// keeping this fixture fully self-contained rather than depending on the
// real org.scalatest jar: this rule doesn't touch the import (that's
// ScalaTestImportChange's job), and the INPUT/OUTPUT projects compile
// against different scalatest versions (3.0.0 vs 3.2.14) where
// org.scalatest.FunSuite only exists in the former, so a real import can't
// round-trip through a single-rule fixture.
trait AnyFunSuite {
}

class OldTest2 extends FunSuite with Farts { // assert: ScalaTestExtendsFix
  val a = 1
}
