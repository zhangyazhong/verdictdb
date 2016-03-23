//SuperSafe for compile-time errors in ScalaTest code
addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.0-RC6")

//Antlr4
resolvers += "simplytyped" at "http://simplytyped.github.io/repo/releases"
addSbtPlugin("com.simplytyped" % "sbt-antlr4" % "0.7.8")

//Assembly
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")
