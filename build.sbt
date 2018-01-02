import Dependencies._

lazy val commonSettings = Seq(
  organization := "tech.sourced",
  scalaVersion := "2.11.12",
  version := "0.1.0"
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := "queryset",
    libraryDependencies ++= Seq(
      SparkSql % Provided,
      SourcedEngine % Compile classifier "slim"
    ),
    dependencyOverrides += SparkGuava
  )

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"
assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
