import AssemblyKeys._

//ScalaTest
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

libraryDependencies += "jline" % "jline" % "2.12.1"
libraryDependencies += "commons-cli" % "commons-cli" % "1.3.1"

// HiveDriver for SparkSQL http://mvnrepository.com/artifact/org.apache.hive/hive-jdbc
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.0.0"

lazy val buildSettings = Seq(
  organization := "edu.umich",
  name := "verdict",
  version := "0.1.0",
  scalaVersion := "2.11.7"
)

lazy val root = (project in file(".")).
  settings(buildSettings: _*).

  //assemblySettings
  settings(assemblySettings: _*).settings(
  assemblyOption in assembly ~= {_.copy(includeScala = false)},
  jarName in assembly := "verdict.jar",
  test in assembly := {},
  mainClass in assembly := Some("edu.umich.verdict.cli.Cli")
  ).

  //antlr4Settings
  settings(antlr4Settings: _*).settings(
  antlr4PackageName in Antlr4 := Some("edu.umich.verdict.parser"),
  antlr4GenListener in Antlr4 := true,
  antlr4GenVisitor in Antlr4 := true
)