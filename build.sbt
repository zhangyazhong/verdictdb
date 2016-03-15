import AssemblyKeys._

libraryDependencies += "jline" % "jline" % "2.12.1"

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
  jarName in assembly := "verdict.jar"
  ).

  //antlr4Settings
  settings(antlr4Settings: _*).settings(
  antlr4PackageName in Antlr4 := Some("edu.umich.verdict.parser"),
  antlr4GenListener in Antlr4 := true,
  antlr4GenVisitor in Antlr4 := true
)