package fix.support

/** The Spark line the job migrates to, for the few rules whose behaviour
 * differs by line (a 3.5 removal a 3.3 target still has). spark-migrate-cli
 * passes it as SPARK_MIGRATE_TARGET_SPARK (`3.3` or `3.5`) to the sbt, Maven
 * or Scalafix process the rules run in; unset means 3.5, the only target there
 * was before the variable existed. */
object TargetSpark {
  val EnvVar = "SPARK_MIGRATE_TARGET_SPARK"

  def line: (Int, Int) =
    sys.env.get(EnvVar).map(_.trim).filter(_.nonEmpty).flatMap { v =>
      """^(\d+)\.(\d+)""".r.findFirstMatchIn(v).map(m => (m.group(1).toInt, m.group(2).toInt))
    }.getOrElse((3, 5))

  /** The target line is `major.minor` or later. */
  def atLeast(major: Int, minor: Int): Boolean = {
    val (a, b) = line
    a > major || (a == major && b >= minor)
  }
}
