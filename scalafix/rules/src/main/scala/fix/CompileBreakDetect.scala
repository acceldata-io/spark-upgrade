package fix

import fix.support.{RuleChange, RuleFinding}
import scalafix.v1._

import scala.collection.mutable
import scala.meta._

/**
 * Source that compiles on Spark 2.4.8 / Scala 2.11 / JDK 8 and does not on
 * Spark 3.5.5 / Scala 2.12 / JDK 17. One table row per break, reported as
 * `CompileBreakDetect.<Sub>`.
 *
 * Tier 1, 3.x-only output (`targetOnly`), where a row has an exact rewrite
 * and the site has the shape it handles; a site that does not is reported as
 * `CompileBreakDetect.<Sub>.Manual` (Tier 3) and left as written. Rows
 * without a rewrite stay Tier 3 (their own registry entries).
 *
 * Every entry was probed 2026-10-13: a tiny sbt project compiles on 2.11.12 +
 * 2.4.8 + JDK 8 and the same source fails on 2.12.18 + 3.5.5 + JDK 17 with
 * the error quoted in the entry's text; the removed members were checked with
 * javap on both jars. Not included because they still compile: mllib
 * KMeansModel.computeCost (only the ml one is gone), a typed lambda or `f _`
 * passed to foreachPartition/foreachBatch, Dataset.foreach with an untyped
 * lambda. SVMWithSGD keeps its train methods.
 *
 * The rewrites, probed 2026-10-14 (2.4.8 sources and javap, then the old call
 * on 2.4.8 against the new one on 3.5.5):
 *  - an untyped lambda's parameters get the types Scala 2.11 inferred
 *    (SemanticDB), so 2.12 picks the same Scala overload; `f(_)` becomes `f _`.
 *  - RowEncoder(schema) -> ExpressionEncoder(schema): 3.5.5's
 *    ExpressionEncoder.apply(StructType) is the ExpressionEncoder[Row] that
 *    RowEncoder.apply built.
 *  - MulticlassMetrics precision/recall/fMeasure -> accuracy: 2.4.8 defines
 *    them as `lazy val precision: Double = accuracy` (likewise the others).
 *  - KMeans: 2.4.8's setRuns only logs and returns this; the train overloads
 *    taking runs build the same KMeans as the ones without it.
 *  - GBT numTrees and getNumTrees are both `trees.length`; StageInfo's
 *    attemptNumber() returns attemptId (javap); MLWriter/MLReader
 *    context(sqlContext) is session(sqlContext.sparkSession); an MLP model's
 *    getLayers on 3.5.5 is the array 2.4.8's layers held (trained and loaded).
 *  - functions.unboundedPreceding()/currentRow()/unboundedFollowing() as
 *    rangeBetween bounds are the boundaries Window.unboundedPreceding/
 *    currentRow/unboundedFollowing give (same rows, same column names). A
 *    lit(n) bound is not rewritten: lit(0) names the column `0 FOLLOWING`
 *    where 0L gives `CURRENT ROW`, and a Long offset fails on a date order.
 *  - databricks .avro(p): on 2.4.8 Spark maps format "com.databricks.spark.avro"
 *    to its built-in Avro source (spark.sql.legacy.replaceDatabricksSparkAvro,
 *    default on; with it off the databricks 4.0.0 jar fails with
 *    AbstractMethodError), so format("avro") is the same source (same schema
 *    and rows for every Spark type, written and read both ways).
 *  - InterfaceStability.X -> org.apache.spark.annotation.X, the same marker.
 * sun.misc.BASE64* is not rewritten in Scala: analysis compiles on JDK 17,
 * where such a file does not compile, so no SemanticDB reaches this rule.
 */
class CompileBreakDetect extends SemanticRule("CompileBreakDetect") {
  override val description =
    "Rewrites Spark 2.4 / Scala 2.11 source that no longer compiles on Spark 3.5.5 / Scala 2.12 where an exact " +
      "rewrite exists, and flags the rest, naming the fix."

  private val ruleId = "CompileBreakDetect"

  /** One document, and the imports its rewrites need (added once, at the end). */
  private case class Ctx(doc: SemanticDocument, imports: mutable.LinkedHashSet[String] = mutable.LinkedHashSet())

  /** At one site: the patch, or why the site is left as written. */
  private type Rewrite = Term.Name => Ctx => Either[String, Patch]

  /** `applies` narrows a symbol match to the broken shape (an overload, a call form).
   *  `done` says what `rewrite` did. */
  private case class Break(sub: String, symbols: Seq[String], why: String, fix: String,
      applies: (Term.Name, SemanticDocument) => Boolean = (_, _) => true,
      rewrite: Option[Rewrite] = None, done: String = "") {
    val matcher: SymbolMatcher = SymbolMatcher.normalized(symbols: _*)
  }

  private def params(t: Tree)(implicit doc: SemanticDocument): List[SymbolInformation] =
    t.symbol.info.toList.flatMap(_.signature match {
      case MethodSignature(_, paramss, _) => paramss.flatten
      case _ => Nil
    })

  private def hasParamOfType(t: Tree, tpe: String)(implicit doc: SemanticDocument): Boolean =
    params(t).exists(_.signature match {
      case ValueSignature(TypeRef(_, sym, _)) => sym.value == tpe
      case _ => false
    })

  // `it => ...`, `{ it => ... }`, `(b, id) => ...`, or placeholder syntax
  // (`f(_)`, `_.size`, `g(_, _)`) with an untyped placeholder. Scala 2.12 cannot
  // type such a lambda against both the Scala function and the Java SAM overload.
  private def untypedFunction(arg: Term): Boolean = arg match {
    case Term.Function(ps, _) => ps.exists(_.decltpe.isEmpty)
    case Term.Block(List(f: Term.Function)) => untypedFunction(f)
    case Term.AnonymousFunction(body) =>
      body.collect { case p: Term.Placeholder if !p.parent.exists(_.is[Term.Ascribe]) => p }.nonEmpty
    case _ => false
  }

  /** A typed lambda (`(batch: DataFrame, id: Long) => ...`) whose body ends in a call returning a value: the
   *  2026-10-19 user trial's `batch.unpersist()`. An untyped one is `ForeachBatchLambda`'s. */
  private def endsInValue(n: Term.Name)(implicit doc: SemanticDocument): Boolean =
    typedFunction(n).flatMap(f => lastStat(f.body)).exists {
      case t @ (_: Term.Apply | _: Term.Select | _: Term.Name) => returnsValue(t)
      case _ => false
    }

  private def typedFunction(n: Term.Name): Option[Term.Function] = soleArg(n) match {
    case Some(f: Term.Function) if f.params.nonEmpty && f.params.forall(_.decltpe.isDefined) => Some(f)
    case Some(Term.Block(List(f: Term.Function))) if f.params.nonEmpty && f.params.forall(_.decltpe.isDefined) => Some(f)
    case _ => None
  }

  private def lastStat(body: Term): Option[Stat] = body match {
    case Term.Block(stats) => stats.lastOption
    case t => Some(t)
  }

  /** The method a call ends in returns something other than Unit. */
  private def returnsValue(t: Tree)(implicit doc: SemanticDocument): Boolean = {
    val method = t match {
      case Term.Apply(fun, _) => fun
      case other => other
    }
    val name = method match {
      case Term.Select(_, n) => n
      case n: Term.Name => n
      case other => other
    }
    // `this.type` (Dataset.unpersist) is a SingleType, not a TypeRef: any return type but Unit, Nothing or none.
    name.symbol.info.exists(_.signature match {
      case MethodSignature(_, _, TypeRef(_, sym, _)) => sym.value != "scala/Unit#" && sym.value != "scala/Nothing#"
      case MethodSignature(_, _, NoType) => false
      case MethodSignature(_, _, _) => true
      case _ => false
    })
  }

  private def appendUnit: Rewrite = n => ctx => {
    typedFunction(n).flatMap(f => lastStat(f.body).map(f -> _)) match {
      case Some((f, last)) =>
        val indent = " " * last.pos.startColumn
        if (f.body.is[Term.Block]) Right(Patch.addRight(last, s"\n$indent()"))
        else Right(Patch.addLeft(last, "{ ") + Patch.addRight(last, "; () }"))
      case None => Left("the lambda's body could not be read")
    }
  }

  // The single argument of `recv.<n>(arg)`.
  private def soleArg(n: Term.Name): Option[Term] = n.parent match {
    case Some(sel @ Term.Select(_, name)) if name eq n =>
      sel.parent.collect { case Term.Apply(fun, List(arg)) if fun eq sel => arg }
    case _ => None
  }

  private def calledWithUntypedFunction(n: Term.Name): Boolean = soleArg(n).exists(untypedFunction)

  // `RowEncoder(schema)` or `RowEncoder.apply(schema)`; other members are untouched.
  private def isApplied(n: Term.Name): Boolean = n.parent.exists {
    case Term.Apply(fun, _) => fun eq n
    case Term.Select(q, Term.Name("apply")) => q eq n
    case _ => false
  }

  // ---- types for lambda parameters ----

  private val inScopeOwners = Set("scala/", "scala/Predef.", "scala/package.", "java/lang/", "_empty_/")
  private val inScopeTypes = Set("scala/collection/Iterator#", "scala/collection/Seq#",
    "scala/collection/immutable/List#", "scala/collection/immutable/Map#")

  /** Source text for a type Scala 2.11 inferred, adding the imports it needs to
   *  `ctx`; None when it cannot be written safely (a nested class this file does
   *  not name, or a name the file already uses for something else). */
  private def typeText(t: SemanticType, ctx: Ctx): Option[String] = {
    implicit val doc: SemanticDocument = ctx.doc
    t match {
      case TypeRef(_, sym, List(TypeRef(_, row, Nil)))
          if sym.value == "org/apache/spark/sql/Dataset#" && row.value == "org/apache/spark/sql/Row#" =>
        named("DataFrame", Symbol("org/apache/spark/sql/package.DataFrame#"), "org.apache.spark.sql.DataFrame", ctx)
      case TypeRef(_, sym, args) =>
        val name = sym.info.map(_.displayName).getOrElse("")
        val head =
          if (inScopeOwners(sym.owner.value) || inScopeTypes(sym.value)) Some(name)
          else if (sym.owner.value.endsWith("/")) named(name, sym, sym.owner.value.stripSuffix("/").replace('/', '.') + "." + name, ctx)
          else if (namesInFile(name).contains(sym)) Some(name)
          else None
        val as = args.map(typeText(_, ctx))
        head.filter(_ => as.forall(_.isDefined)).map { h =>
          if (as.isEmpty) h
          else if (sym.value.startsWith("scala/Tuple")) as.flatten.mkString("(", ", ", ")")
          else as.flatten.mkString(s"$h[", ", ", "]")
        }
      case _ => None
    }
  }

  // What `name`, written unqualified, refers to in this file.
  private def namesInFile(name: String)(implicit doc: SemanticDocument): Set[Symbol] =
    doc.tree.collect {
      case n: Type.Name if n.value == name && !n.isDefinition && !n.parent.exists(_.is[Type.Select]) => n.symbol
    }.toSet

  // A top-level type by its simple name, imported unless the file already names
  // it; refused if the file uses the name for something else.
  private def named(name: String, sym: Symbol, path: String, ctx: Ctx): Option[String] = {
    val used = namesInFile(name)(ctx.doc)
    if (used.contains(sym)) Some(name)
    else if (used.isEmpty) { ctx.imports += path; Some(name) }
    else None
  }

  /** `it => ...` / `{ (b, id) => ... }`: write the inferred types on the parameters;
   *  `f(_)` / `f(_, _)` naming a method: `f _`. */
  private def typeLambda: Rewrite = n => ctx => {
    implicit val doc: SemanticDocument = ctx.doc
    def function(f: Term.Function): Either[String, Patch] = {
      val typed = f.params.map(p => p -> p.name.symbol.info.map(_.signature).collect {
        case ValueSignature(tpe) => tpe
      }.flatMap(typeText(_, ctx)))
      if (typed.exists(_._2.isEmpty)) Left("a parameter's type could not be written in this file")
      else {
        val parens = f.tokens.head.is[Token.LeftParen]
        Right(typed.collect { case (p, Some(text)) =>
          if (parens) Patch.addRight(p.name.tokens.filter(_.is[Token.Ident]).last, s": $text") else Patch.replaceTree(p, s"(${p.syntax}: $text)")
        }.asPatch)
      }
    }
    soleArg(n) match {
      case Some(f: Term.Function) => function(f)
      case Some(Term.Block(List(f: Term.Function))) => function(f)
      case Some(a @ Term.AnonymousFunction(Term.Apply(m: Term.Name, args)))
          if args.forall(_.is[Term.Placeholder]) && isPlainMethod(m, args.size) =>
        Right(Patch.replaceTree(a, s"${m.syntax} _"))
      case _ => Left("a placeholder function other than f(_) for a method f has no type to write")
    }
  }

  // A method with this one parameter list, not overloaded, so `m _` names it.
  private def isPlainMethod(m: Term.Name, arity: Int)(implicit doc: SemanticDocument): Boolean =
    m.symbol.info.exists(_.signature match {
      case MethodSignature(Nil, List(ps), _) if ps.size == arity =>
        m.symbol.owner.info.exists(_.signature match {
          case ClassSignature(_, _, _, decls) => decls.count(_.displayName == m.value) == 1
          case _ => false
        })
      case _ => false
    })

  // ---- the other rewrites ----

  private def rename(to: String): Rewrite = n => _ =>
    if (n.parent.exists { case Term.Select(_, name) => name eq n; case _ => false }) Right(Patch.replaceTree(n, to))
    else Left(s"it is not written as x.${n.value}")

  // A literal or a name: dropping it drops no side effect.
  private def isPure(t: Term): Boolean = t.is[Lit] || t.is[Term.Name]

  private def kmeansRuns: Rewrite = n => _ => (n.value, n.parent) match {
    case ("setRuns", Some(sel @ Term.Select(recv, _))) => sel.parent match {
      case Some(app @ Term.Apply(fun, List(arg))) if (fun eq sel) && isPure(arg) =>
        Right(Patch.removeTokens(app.tokens.filter(_.start >= recv.pos.end)))
      case _ => Left("only .setRuns(<a literal or a name>) is dropped")
    }
    case ("train", Some(sel: Term.Select)) => sel.parent match {
      case Some(app @ Term.Apply(fun, args)) if fun eq sel =>
        val i = args.indexWhere { case Term.Assign(Term.Name("runs"), _) => true; case _ => false } match {
          case -1 => 3
          case named => named
        }
        args.lift(i).filter { case Term.Assign(_, v) => isPure(v); case a => isPure(a) } match {
          case Some(runs) =>
            Right(Patch.removeTokens(app.tokens.filter(t => t.start >= args(i - 1).pos.end && t.end <= runs.pos.end)))
          case None => Left("only a literal or a name as the runs argument is removed")
        }
      case _ => Left("only a call KMeans.train(...) is rewritten")
    }
    case _ => Left("KMeans.getRuns returned 1 in 2.4 (and logged a warning)")
  }

  private def mlContext: Rewrite = n => _ => soleArg(n) match {
    case Some(arg) =>
      val recv = if (arg.is[Term.Name] || arg.is[Term.Select] || arg.is[Term.Apply]) arg.syntax else s"(${arg.syntax})"
      Right(Patch.replaceTree(n, "session") + Patch.replaceTree(arg, s"$recv.sparkSession"))
    case None => Left("only .context(sqlContext) applied to one argument is rewritten")
  }

  private val annotations = Set("Stable", "Evolving", "Unstable")

  private def interfaceStability: Rewrite = n => ctx => n.parent match {
    case Some(sel @ Type.Select(q, Type.Name(a))) if (q eq n) && annotations(a) =>
      ctx.imports += s"org.apache.spark.annotation.$a"
      Right(Patch.replaceTree(sel, a))
    case _ => Left("only InterfaceStability.Stable / Evolving / Unstable is rewritten")
  }

  private val boundaries = Map(
    "unboundedPreceding" -> "Window.unboundedPreceding", "currentRow" -> "Window.currentRow",
    "unboundedFollowing" -> "Window.unboundedFollowing")
  private val boundaryFunctions =
    SymbolMatcher.normalized(boundaries.keys.toSeq.map("org.apache.spark.sql.functions." + _): _*)

  // `unboundedPreceding()`, `functions.currentRow()`, or either without parentheses.
  private def boundary(t: Term)(implicit doc: SemanticDocument): Option[String] = t match {
    case Term.Apply(f, Nil) => boundary(f)
    case Term.Select(_, name) => boundary(name)
    case name: Term.Name if boundaryFunctions.matches(name) => boundaries.get(name.value)
    case _ => None
  }

  // rangeBetween(a, b) with a boundary function for both bounds: the Long overload.
  private def rangeBetween(call: Tree, ctx: Ctx): Either[String, Patch] = call match {
    case Term.Apply(_, List(a, b)) => (boundary(a)(ctx.doc), boundary(b)(ctx.doc)) match {
      case (Some(x), Some(y)) =>
        ctx.imports += "org.apache.spark.sql.expressions.Window"
        Right(Patch.replaceTree(a, x) + Patch.replaceTree(b, y))
      case _ => Left("a bound other than unboundedPreceding() / currentRow() / unboundedFollowing() (lit(n) " +
        "included) can name or type the frame differently as a Long")
    }
    case _ => Left("only rangeBetween(start, end) is rewritten")
  }

  private def rangeBetweenRewrite: Rewrite = n => ctx => n.parent.flatMap(_.parent) match {
    case Some(call) => rangeBetween(call, ctx)
    case None => Left("only x.rangeBetween(start, end) is rewritten")
  }

  // A boundary function is rewritten with the rangeBetween call it is a bound of.
  private def boundaryRewrite: Rewrite = n => ctx => {
    val arg = n.parent match {
      case Some(a @ Term.Apply(f, Nil)) if f eq n => a
      case Some(s @ Term.Select(_, name)) if name eq n =>
        s.parent.collect { case a @ Term.Apply(f, Nil) if f eq s => a }.getOrElse(s)
      case _ => n
    }
    arg.parent match {
      case Some(c @ Term.Apply(Term.Select(_, Term.Name("rangeBetween")), _)) => rangeBetween(c, ctx).map(_ => Patch.empty)
      case _ => Left("only a bound of rangeBetween(start, end) is rewritten")
    }
  }

  private val breaks: Seq[Break] = Seq(
    Break("ForeachPartitionLambda", Seq("org.apache.spark.sql.Dataset.foreachPartition"),
      "Scala 2.12 resolves an untyped lambda passed to Dataset.foreachPartition to the Java ForeachPartitionFunction " +
        "overload, so its parameter is a java.util.Iterator and the call fails to compile (\"value size is not a " +
        "member of Object\", \"type mismatch\").",
      "Give the lambda's parameter its type: ds.foreachPartition((it: Iterator[T]) => ...), with T the Dataset's " +
        "element type (Row for a DataFrame); write f(_) as f _.",
      (n, doc) => calledWithUntypedFunction(n), Some(typeLambda),
      "Wrote the parameter's type, Iterator[T] as Scala 2.11 inferred it, so Scala 2.12 calls the same overload " +
        "(f(_) became f _)."),
    Break("ForeachBatchLambda", Seq("org.apache.spark.sql.streaming.DataStreamWriter.foreachBatch"),
      "Scala 2.12 cannot choose between DataStreamWriter.foreachBatch's Scala-function and Java VoidFunction2 " +
        "overloads for an untyped lambda: \"ambiguous reference to overloaded definition\".",
      "Type the parameters: foreachBatch((batch: DataFrame, batchId: Long) => ...), or pass a method as " +
        "foreachBatch(process _) instead of process(_, _).",
      (n, doc) => calledWithUntypedFunction(n), Some(typeLambda),
      "Wrote the parameters' types, (DataFrame, Long) as Scala 2.11 inferred them, so Scala 2.12 calls the same " +
        "overload (process(_, _) became process _)."),
    Break("ForeachBatchValueBody", Seq("org.apache.spark.sql.streaming.DataStreamWriter.foreachBatch"),
      "Scala 2.12 cannot choose between DataStreamWriter.foreachBatch's Scala-function and Java VoidFunction2 " +
        "overloads for a lambda whose body ends in a value (batch.unpersist() returns the Dataset), even with typed " +
        "parameters: \"ambiguous reference to overloaded definition\".",
      "End the lambda's body with () so it returns Unit.",
      (n, doc) => endsInValue(n)(doc), Some(appendUnit),
      "Ended the body with (): the lambda returned Unit on Scala 2.11 too (foreachBatch discards the value), so " +
        "Scala 2.12 calls the same overload."),
    Break("RowEncoderApply", Seq("org.apache.spark.sql.catalyst.encoders.RowEncoder"),
      "RowEncoder(schema) / RowEncoder.apply(schema) was removed in Spark 3.5; the object only has encoderFor now.",
      "Use ExpressionEncoder(schema) (org.apache.spark.sql.catalyst.encoders.ExpressionEncoder, the same " +
        "ExpressionEncoder[Row]), or Encoders.row(schema) where an Encoder[Row] is enough.",
      (n, doc) => isApplied(n),
      Some(n => ctx => {
        ctx.imports += "org.apache.spark.sql.catalyst.encoders.ExpressionEncoder"
        Right(Patch.replaceTree(n, "ExpressionEncoder"))
      }),
      "Replaced with ExpressionEncoder(schema), which builds the same ExpressionEncoder[Row] in 3.5."),
    Break("MulticlassMetricsNoLabel",
      Seq("precision", "recall", "fMeasure").map("org.apache.spark.mllib.evaluation.MulticlassMetrics." + _),
      "MulticlassMetrics.precision, recall and fMeasure without a label argument were removed in Spark 3.0; in 2.4 " +
        "each one returned the overall accuracy.",
      "Replace it with metrics.accuracy, the value it returned in 2.4 (or pass a label for a per-label value).",
      (n, doc) => params(n)(doc).isEmpty, Some(rename("accuracy")),
      "Replaced with accuracy: 2.4.8 defined the no-label precision, recall and fMeasure as accuracy."),
    Break("KMeansRuns",
      Seq("setRuns", "getRuns", "train").map("org.apache.spark.mllib.clustering.KMeans." + _),
      "The mllib KMeans runs parameter was removed in Spark 3.0 (setRuns, getRuns and the train overloads taking " +
        "runs). It had no effect since Spark 2.0.",
      "Drop it: delete .setRuns(n), and remove the runs argument from KMeans.train(data, k, maxIterations, runs, ...).",
      (n, doc) => n.value != "train" || params(n)(doc).exists(_.displayName == "runs"), Some(kmeansRuns),
      "Dropped .setRuns(n) / the runs argument: 2.4.8 ignored runs, so the same KMeans is built."),
    Break("GbtNumTrees",
      Seq("org.apache.spark.ml.classification.GBTClassificationModel.numTrees",
        "org.apache.spark.ml.regression.GBTRegressionModel.numTrees"),
      "GBTClassificationModel.numTrees and GBTRegressionModel.numTrees were removed in Spark 3.0.",
      "Use getNumTrees, which returns the same number.",
      rewrite = Some(rename("getNumTrees")), done = "Replaced with getNumTrees, the same trees.length."),
    Break("StageInfoAttemptId", Seq("org.apache.spark.scheduler.StageInfo.attemptId"),
      "StageInfo.attemptId is private from Spark 3.0 (\"cannot be accessed\").",
      "Use attemptNumber(), which returns the same value.",
      rewrite = Some(rename("attemptNumber()")), done = "Replaced with attemptNumber(), which returns attemptId."),
    Break("InterfaceStability", Seq("org.apache.spark.annotation.InterfaceStability"),
      "org.apache.spark.annotation.InterfaceStability was removed in Spark 3.0; its Stable, Evolving and Unstable " +
        "annotations moved up one level.",
      "Use org.apache.spark.annotation.Stable / Evolving / Unstable, and drop the InterfaceStability import.",
      rewrite = Some(interfaceStability),
      done = "Replaced with org.apache.spark.annotation.Stable / Evolving / Unstable (imported)."),
    Break("MlReadWriteContext",
      Seq("MLWriter", "MLReader", "GeneralMLWriter", "BaseReadWrite").map("org.apache.spark.ml.util." + _ + ".context"),
      "MLWriter.context(sqlContext) and MLReader.context(sqlContext) were removed in Spark 3.0.",
      "Use .session(sqlContext.sparkSession), or .session(spark) with your SparkSession.",
      rewrite = Some(mlContext),
      done = "Replaced with .session(sqlContext.sparkSession), which is what context(sqlContext) did in 2.4.8."),
    Break("WindowBoundaryFunctions",
      Seq("currentRow", "unboundedPreceding", "unboundedFollowing").map("org.apache.spark.sql.functions." + _),
      "functions.currentRow(), unboundedPreceding() and unboundedFollowing() were removed in Spark 3.0, as was the " +
        "rangeBetween(Column, Column) overload they were made for.",
      "Use the Long boundaries with rangeBetween(Long, Long) or rowsBetween: Window.currentRow, " +
        "Window.unboundedPreceding, Window.unboundedFollowing.",
      rewrite = Some(boundaryRewrite),
      done = "Replaced with Window.unboundedPreceding / currentRow / unboundedFollowing, the same frame boundary."),
    Break("RangeBetweenColumns",
      Seq("org.apache.spark.sql.expressions.Window.rangeBetween", "org.apache.spark.sql.expressions.WindowSpec.rangeBetween"),
      "rangeBetween(Column, Column) was removed in Spark 3.0; only rangeBetween(Long, Long) remains.",
      "Pass Long boundaries: Window.unboundedPreceding / Window.currentRow / Window.unboundedFollowing or a literal " +
        "offset, e.g. rangeBetween(Window.unboundedPreceding, Window.currentRow).",
      (n, doc) => hasParamOfType(n, "org/apache/spark/sql/Column#")(doc), Some(rangeBetweenRewrite),
      "Now calls rangeBetween(Long, Long) with the Window boundaries: the same frame, rows and column name."),
    Break("SunMiscBase64", Seq("sun.misc.BASE64Encoder", "sun.misc.BASE64Decoder"),
      "sun.misc.BASE64Encoder and BASE64Decoder were removed in JDK 9; Spark 3.5.5 runs on JDK 17.",
      "Decoding: java.util.Base64.getMimeDecoder().decode(s) gives the same bytes for well-formed input (sun's " +
        "decoder returned other bytes for unpadded or malformed input). Encoding: getMimeEncoder(76, " +
        "System.lineSeparator.getBytes).encodeToString(bytes) gives encode's lines; encode also ended with a line break when the " +
        "input length was a non-zero multiple of 57, encodeBuffer after any non-empty input."),
    Break("MlKMeansComputeCost", Seq("org.apache.spark.ml.clustering.KMeansModel.computeCost"),
      "ml KMeansModel.computeCost(dataset) was removed in Spark 3.0.",
      "For the training data's cost use model.summary.trainingCost. For other data, ClusteringEvaluator gives the " +
        "silhouette (a different metric); the same cost needs the mllib KMeansModel(centers).computeCost(rdd)."),
    Break("SparkHadoopUtil", Seq("org.apache.spark.deploy.SparkHadoopUtil"),
      "org.apache.spark.deploy.SparkHadoopUtil is private[spark] from Spark 3.0, so Scala code can no longer reach it.",
      "Use the Hadoop APIs directly: spark.sparkContext.hadoopConfiguration for its conf, " +
        "org.apache.hadoop.security.UserGroupInformation for user and credential calls, FileSystem for paths."),
    Break("ImageSchemaReadImages", Seq("org.apache.spark.ml.image.ImageSchema.readImages"),
      "ImageSchema.readImages was removed in Spark 3.0.",
      "Use the image data source: spark.read.format(\"image\").load(path) (option dropInvalid for dropImageFailures, " +
        "recursiveFileLookup for recursive)."),
    Break("MlpLayers", Seq("org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel.layers"),
      "MultilayerPerceptronClassificationModel.layers is a Param from Spark 3.0, no longer an Array[Int], so array " +
        "use of it fails to compile.",
      "Use getLayers, the Array[Int] layers used to be.",
      rewrite = Some(rename("getLayers")), done = "Replaced with getLayers, the same Array[Int]."),
    Break("AccumulableApi",
      Seq("org.apache.spark.AccumulatorParam", "org.apache.spark.AccumulableParam", "org.apache.spark.Accumulable",
        "org.apache.spark.SparkContext.accumulable", "org.apache.spark.SparkContext.accumulableCollection"),
      "The v1 accumulator API (AccumulatorParam, AccumulableParam, Accumulable, SparkContext.accumulable and " +
        "accumulableCollection) was removed in Spark 3.0.",
      "Extend org.apache.spark.util.AccumulatorV2 and register it with sc.register(acc, name); for a collection use " +
        "sc.collectionAccumulator[T](name)."),
    Break("MllibWithSgd",
      Seq("org.apache.spark.mllib.classification.LogisticRegressionWithSGD",
        "org.apache.spark.mllib.regression.LinearRegressionWithSGD",
        "org.apache.spark.mllib.regression.RidgeRegressionWithSGD",
        "org.apache.spark.mllib.regression.LassoWithSGD"),
      "LogisticRegressionWithSGD, LinearRegressionWithSGD, RidgeRegressionWithSGD and LassoWithSGD lost their " +
        "train(...) methods and public constructor in Spark 3.0.",
      "Use LogisticRegressionWithLBFGS, or the spark.ml estimators (LogisticRegression, LinearRegression with " +
        "elasticNetParam 0 for ridge or 1 for lasso).")
  )

  private val avroPackage = "com/databricks/spark/avro/"

  override def fix(implicit doc: SemanticDocument): Patch = {
    val ctx = Ctx(doc)
    val avroImports = doc.tree.collect { case i: Importer if i.ref.syntax == "com.databricks.spark.avro" => i }
    val avroCalls = doc.tree.collect {
      case n @ Term.Name("avro") if n.parent.exists(_.is[Term.Select]) && !inImport(n) &&
          (n.symbol.value.startsWith(avroPackage) || (n.symbol.isNone && avroImports.nonEmpty)) => n
    }
    val otherAvroUse = doc.tree.collect {
      case n: Name if !inImport(n) && !n.isDefinition && !avroCalls.contains(n) && n.symbol.value.startsWith(avroPackage) &&
          !ancestors(n).exists { case Pkg.Object(_, Term.Name("avro"), _) => true; case _ => false } => n
    }.nonEmpty
    val avroDone = !otherAvroUse && avroCalls.forall(avroRewrite(_).isRight)
    // The import of a removed name goes when no use of it is left as written.
    def allRewritten(sub: String): Boolean = {
      val b = breaks.find(_.sub == sub).get
      doc.tree.collect { case n: Name if !inImport(n) && matches(b, n) => n }.forall {
        case n: Term.Name => b.rewrite.get(n)(Ctx(doc)).isRight
        case _ => false
      }
    }
    val stale = Seq("org.apache.spark.annotation" -> Set("InterfaceStability")).filter(_ => allRewritten("InterfaceStability")) ++
      Seq("org.apache.spark.sql.functions" -> boundaries.keySet).filter(_ => allRewritten("WindowBoundaryFunctions"))
    val staleImports = doc.tree.collect {
      case Importer(ref, importees) => stale.collect { case (pkg, names) if ref.syntax == pkg =>
        importees.collect { case i @ Importee.Name(Name(v)) if names(v) => Patch.removeImportee(i) }.asPatch
      }.asPatch
    }.asPatch
    val sites = doc.tree.collect {
      case i: Importer if avroImports.contains(i) => avroImport(i, avroDone)
      case n: Term.Name if avroCalls.contains(n) => avro(n)
      case n: Term.Name if !inImport(n) => report(n, ctx)
      case t: Type.Name if !inImport(t) => report(t, ctx)
    }.asPatch
    val utils = Utils()
    staleImports + sites + ctx.imports.toList.map(p => utils.addImportIfNotPresent(dialects.Scala212(p).parse[Importer].get)).asPatch
  }

  private def ancestors(t: Tree): List[Tree] = t.parent.toList.flatMap(p => p :: ancestors(p))

  // Imports are not reported: the uses below them are where the fix goes.
  private def inImport(t: Tree): Boolean = t.parent.exists(p => p.is[Import] || inImport(p))

  private def matches(b: Break, n: Tree)(implicit doc: SemanticDocument): Boolean =
    b.matcher.matches(n) && (n match {
      case t: Term.Name => b.applies(t, doc)
      case _ => true
    })

  private def report(n: Tree, ctx: Ctx)(implicit doc: SemanticDocument): Patch =
    breaks.find(matches(_, n)).fold(Patch.empty) { b =>
      val id = s"$ruleId.${b.sub}"
      (b.rewrite, n) match {
        case (Some(rewrite), t: Term.Name) => rewrite(t)(ctx) match {
          case Right(patch) => RuleFinding.report(RuleChange(id, b.why, b.done, n), patch)
          case Left(reason) => RuleFinding.report(RuleChange(s"$id.Manual", b.why, s"Not rewritten: $reason. ${b.fix}", n))
        }
        case (Some(_), _) => RuleFinding.report(RuleChange(s"$id.Manual", b.why, s"Not rewritten. ${b.fix}", n))
        case _ => RuleFinding.report(RuleChange(id, b.why, b.fix, n))
      }
    }

  private val avroWhy =
    "com.databricks spark-avro has no Scala 2.12 build, so `import com.databricks.spark.avro._` and its implicit " +
      ".avro(path) on DataFrameReader/DataFrameWriter do not compile on Spark 3.5.5. Avro is built into Spark " +
      "(org.apache.spark:spark-avro); Spark 2.4 already ran that built-in source for format " +
      "\"com.databricks.spark.avro\" (spark.sql.legacy.replaceDatabricksSparkAvro.enabled)."

  // `reader.avro(p)` -> `reader.format("avro").load(p)`, `writer.avro(p)` -> `writer.format("avro").save(p)`.
  private def avroRewrite(n: Term.Name)(implicit doc: SemanticDocument): Either[String, Patch] =
    soleArg(n) match {
      case Some(_) if !n.symbol.isNone =>
        val op = if (n.symbol.owner.value.contains("Writer")) "save" else "load"
        Right(Patch.replaceTree(n, s"""format("avro").$op"""))
      case _ => Left(".avro is not applied to a path here, or its type did not resolve")
    }

  private def avro(n: Term.Name)(implicit doc: SemanticDocument): Patch = avroRewrite(n) match {
    case Right(patch) =>
      RuleFinding.report(RuleChange(s"$ruleId.DatabricksAvro", avroWhy,
        "Replaced .avro(path) with .format(\"avro\").load(path) on a reader, .format(\"avro\").save(path) on a " +
          "writer: the same built-in Avro source.", n), patch)
    case Left(reason) =>
      RuleFinding.report(RuleChange(s"$ruleId.DatabricksAvro.Manual", avroWhy,
        "Not rewritten: " + reason + ". Replace each .avro(path) with .format(\"avro\").load(path) on a reader, " +
          ".format(\"avro\").save(path) on a writer.", n))
  }

  private def avroImport(i: Importer, done: Boolean)(implicit doc: SemanticDocument): Patch =
    if (done)
      RuleFinding.report(RuleChange(s"$ruleId.DatabricksAvro", avroWhy, "Deleted the import: every .avro call was rewritten.", i),
        i.importees.map(Patch.removeImportee).asPatch)
    else
      RuleFinding.report(RuleChange(s"$ruleId.DatabricksAvro.Manual", avroWhy,
        "Not rewritten: the file uses the library beyond .avro(path) calls the tool could rewrite. Delete the " +
          "import once nothing in the file needs it.", i))
}
