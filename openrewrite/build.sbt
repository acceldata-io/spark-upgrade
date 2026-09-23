// Standalone build, same shape as ../iceberg-spark-upgrade-wap-plugin -- this
// repo has no unified root build.sbt; every module (scalafix/, pysparkler/,
// sql/, iceberg-spark-upgrade-wap-plugin/) builds independently and is
// published/consumed by coordinate, not by sbt aggregation.
//
// Pure Java: OpenRewrite's own APIs (rewrite-core/rewrite-java) are Java, and
// there is no Scala dependency here at all (unlike the scalafix rules jar,
// which needs Scala to depend on scalafix-core) -- crossPaths/autoScalaLibrary
// are both off so the published artifact carries no `_2.12` suffix.
//
// See spark-migrate-java-openrewrite-integration-2026-09-18.md SS8.4: this
// keeps "one place holds all migration knowledge, spark-migrate-cli only
// orchestrates" intact for Java the same way it already does for Scala/SQL/
// PySpark.
ThisBuild / organization := "com.acceldata"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18" // sbt itself needs a Scala version pinned; no Scala source uses it

// OpenRewrite's own docs caution that the parser/recipe-execution APIs are
// not guaranteed stable across releases (spark-migrate-java-openrewrite-
// integration-2026-09-18.md SS2.5 item 2) -- pin an exact version
// deliberately, verified against Maven Central at the time this module was
// written (2026-09-21), not "latest".
val openRewriteVersion = "8.56.1"
val rewriteMigrateJavaVersion = "3.12.0"

lazy val root = (project in file("."))
  .settings(
    name := "spark-openrewrite-rules",
    crossPaths := false,
    autoScalaLibrary := false,
    javacOptions ++= Seq("-source", "17", "-target", "17"),
    libraryDependencies ++= Seq(
      "org.openrewrite" % "rewrite-core" % openRewriteVersion,
      "org.openrewrite" % "rewrite-java" % openRewriteVersion,
      // Parser for the JDK-17 language level -- parses 2.4.8-era Java 8
      // source fine (17 is a syntactic superset) and lets recipes resolve
      // types added through 17.
      "org.openrewrite" % "rewrite-java-17" % openRewriteVersion,
      // Ships org.openrewrite.java.migrate.UpgradeToJava17, resolved at
      // runtime via Environment.scanRuntimeClasspath() (RecipeCatalog) --
      // confirmed against a real spike this session, not assumed from docs.
      "org.openrewrite.recipe" % "rewrite-migrate-java" % rewriteMigrateJavaVersion,
      // Result.diff() shells jgit internally (InMemoryDiffEntry), which needs
      // slf4j-api at runtime -- confirmed the hard way via `cs launch`
      // (NoClassDefFoundError: org/slf4j/LoggerFactory) when nothing in this
      // module's own dependency graph pulled it in. A no-op binding, not a
      // real logging dependency: this is a CLI runner, not a service.
      "org.slf4j" % "slf4j-nop" % "1.7.36",
      "org.openrewrite" % "rewrite-test" % openRewriteVersion % Test,
      "org.junit.jupiter" % "junit-jupiter" % "5.10.2" % Test,
      "org.assertj" % "assertj-core" % "3.25.3" % Test,
      // Real 2.4.8 jars on the RewriteTest classpath, so MethodMatcher type
      // attribution actually resolves Dataset/functions the way it would
      // against a real target repo -- same "input depends on the SOURCE
      // Spark version" convention ../scalafix's own input/ project uses.
      // Plain `%`, not `%%`: Java doesn't care about the artifact's own
      // Scala-binary suffix, only its compiled API.
      "org.apache.spark" % "spark-sql_2.11" % "2.4.8" % Test
    ),
    // Runs JUnit5 tests under sbt (RewriteTest specs are JUnit5, not
    // ScalaTest) -- needs the matching project/plugins.sbt entry too.
    libraryDependencies += "com.github.sbt.junit" % "jupiter-interface" % "0.15.1" % Test,
    // javadoc's strict Java-17 HTML/doclint checking chokes on this module's
    // Javadoc comments; not worth fighting for a local-only SNAPSHOT jar.
    Compile / packageDoc / publishArtifact := false
  )

Compile / mainClass := Some("com.acceldata.openrewrite.Runner")
