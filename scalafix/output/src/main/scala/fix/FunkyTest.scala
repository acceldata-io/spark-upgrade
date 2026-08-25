 
import org.scalatest.funsuite.{ AnyFunSuite, AnyFunSuiteLike }
import org.scalatest.matchers.should.Matchers._ 

class OldTest extends AnyFunSuite { val a = 1 } 

// A grouped import -- the old whole-statement quasiquote required exactly
// one importee and silently missed this shape entirely.
import org.scalatest.Suite 

class OldTest3 extends AnyFunSuiteLike with Suite { val b = 2 } 
