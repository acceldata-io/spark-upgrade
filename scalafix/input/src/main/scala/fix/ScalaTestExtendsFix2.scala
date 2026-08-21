/*
 rule=ScalaTestExtendsFix
 */
trait Farts {
}

trait FunSuite { // assert: ScalaTestExtendsFix
}

class OldTest2 extends FunSuite with Farts { // assert: ScalaTestExtendsFix
  val a = 1
}
