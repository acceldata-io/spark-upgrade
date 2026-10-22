package scalateststyles
 
import org.scalatest.BeforeAndAfter 
import org.scalatest.flatspec.{ AnyFlatSpec, AsyncFlatSpec }
import org.scalatest.wordspec.AnyWordSpec 

// ScalaTest 3.2 removed every 3.0 suite style from org.scalatest, not only FunSuite.
class FlatTest extends AnyFlatSpec { val a = 1 } 

class WordTest extends AnyWordSpec with BeforeAndAfter { val b = 2 } 

// The Async styles keep their names; only the package moves.
class AsyncTest extends AsyncFlatSpec { val c = 3 }
