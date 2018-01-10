import sbt._

object Dependencies {
  lazy val SparkSql = "org.apache.spark" %% "spark-sql" % "2.2.0"
  lazy val SourcedEngine = "tech.sourced" % "engine" % "0.3.4"
}
