import Dependencies._

lazy val commonSettings = Seq(
  organization := "tech.sourced",
  scalaVersion := "2.11.12",
  version := "0.2.0"
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := "queryset",
    libraryDependencies ++= Seq(
      SparkSql % Provided,
      SourcedEngine % Compile classifier "slim"
    )
  )

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"
assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "com.google.shadedcommon.@1").inAll,
  ShadeRule.rename("io.netty.**" -> "io.shadednetty.@1").inAll
)
