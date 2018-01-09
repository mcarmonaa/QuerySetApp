package tech.sourced.queryset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import tech.sourced.engine._


object Main extends App {

  val spark = SparkSession.builder().getOrCreate()

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger(classOf[tech.sourced.engine.provider.RepositoryProvider]).setLevel(Level.ERROR)

  val reposPath = args(0)
  val reposFormat = args(1)

  val engine = Engine(spark, reposPath, reposFormat)

  BoaQueries(spark).run(engine)
  SourcedQueries(spark).run(engine)

}
