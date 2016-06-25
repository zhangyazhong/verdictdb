import sbtassembly.AssemblyPlugin.autoImport._


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
  settings(
  assemblyOption in assembly ~= {
    _.copy(includeScala = false)
  },
  assemblyJarName in assembly := "verdict.jar",
  test in assembly := {},
  mainClass in assembly := Some("edu.umich.verdict.cli.Cli"),
  assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
    case PathList("javax", "activation", xs@_*) => MergeStrategy.last
    case PathList("org", "apache", xs@_*) => MergeStrategy.last
    case PathList("com", "google", xs@_*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
    case PathList("com", "codahale", xs@_*) => MergeStrategy.last
    case PathList("com", "yammer", xs@_*) => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
    case "META-INF/mailcap" => MergeStrategy.last
    case "META-INF/mimetypes.default" => MergeStrategy.last
    case "plugin.properties" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.last
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value(x)
      if (oldStrategy != MergeStrategy.deduplicate) oldStrategy else MergeStrategy.last
  }
).

  //antlr4Settings
  settings(antlr4Settings: _*).settings(
  antlr4PackageName in Antlr4 := Some("edu.umich.verdict.parser"),
  antlr4GenListener in Antlr4 := true,
  antlr4GenVisitor in Antlr4 := true
)