package fix
import fix.support.{RuleChange, RuleFinding}
import metaconfig.generic.Surface
import metaconfig.{ConfDecoder, Configured}
import scalafix.v1._

import scala.meta._
final case class UnionRewriteConfig(
  deprecatedMethod: Map[String, String]
)

object UnionRewriteConfig {
  val default: UnionRewriteConfig =
    UnionRewriteConfig(
      deprecatedMethod = Map(
        "unionAll" -> "union"
      )
    )

  implicit val surface: Surface[UnionRewriteConfig] =
    metaconfig.generic.deriveSurface[UnionRewriteConfig]
  implicit val decoder: ConfDecoder[UnionRewriteConfig] =
    metaconfig.generic.deriveDecoder(default)
}

class UnionRewrite(config: UnionRewriteConfig) extends SemanticRule("UnionRewrite") {
  def this() = this(UnionRewriteConfig.default)

  override def withConfiguration(config: Configuration): Configured[Rule] =
    config.conf.getOrElse("UnionRewrite")(this.config).map { newConfig =>
      new UnionRewrite(newConfig)
    }

  override val isRewrite = true

  override def fix(implicit doc: SemanticDocument): Patch = {
    val ruleId = "UnionRewrite"
    val explanation = "unionAll is deprecated; use union instead (identical semantics)."

    def renamed(nameTree: Tree, replacement: String): Patch =
      RuleFinding.report(
        RuleChange(ruleId, explanation, s"Rewrote to $replacement", nameTree),
        Patch.replaceTree(nameTree, replacement)
      )

    def matchOnTree(t: Tree): Patch = {
      t.collect {
        case Term.Apply(
            Term.Select(_, deprecated @ Term.Name(name)),
            _
            ) if config.deprecatedMethod.contains(name) =>
          renamed(deprecated, config.deprecatedMethod(name))
        // `"reduce" == name`, not `"reduce".contains(name)`: the latter asked whether
        // the method name is a SUBSTRING of "reduce", matching `.re(...)`, `.red(...)`,
        // `.uce(...)` and any other substring of it, on any receiver type.
        case Term.Apply(
            Term.Select(_, _ @Term.Name(name)),
            List(
              Term.AnonymousFunction(
                Term.ApplyInfix(
                  _,
                  deprecatedAnm @ Term.Name(nameAnm),
                  _,
                  _
                )
              )
            )
            ) if "reduce" == name && config.deprecatedMethod.contains(nameAnm) =>
          renamed(deprecatedAnm, config.deprecatedMethod(nameAnm))
      }.asPatch
    }

    matchOnTree(doc.tree)
  }
}
