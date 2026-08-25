package fix

object ProcedureSyntaxDetectExample {
  // At risk: classic procedure syntax, no `=`, no declared type.
  def logStart(): Unit = { 
    println("starting")
  }

  // At risk: procedure syntax with a parameter and a multi-statement body.
  def logMessage(msg: String): Unit = { 
    println("[log] " + msg)
    println("done")
  }

  // At risk: procedure syntax with no parameter list parens at all is not
  // legal Scala (a def always needs at least `()` or nothing before `{` is
  // ambiguous with a val) -- procedure syntax always has an explicit `()`.
  def noArgProcedure(): Unit = { 
    ()
  }

  // Not at risk: this LOOKS identical in tree shape (no declared type,
  // Term.Block body) but has a real `=` -- an entirely ordinary method
  // whose return type happens to be inferred. This is the exact false
  // positive a naive AST-shape-only match would produce.
  def computeTotal() = {
    val a = 1
    val b = 2
    a + b
  }

  // Not at risk: explicit declared return type AND `=` -- ordinary method.
  def explicitUnit(): Unit = {
    println("fine")
  }

  // Not at risk: single-expression body, no braces at all -- not procedure
  // syntax by construction (no Term.Block).
  def addOne(x: Int) = x + 1
}
