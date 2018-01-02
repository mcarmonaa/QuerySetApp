package tech.sourced.queryset

import org.apache.spark.sql.SparkSession
import tech.sourced.engine.Engine

trait QueryExecutor {

  def queries: Seq[(Engine, SparkSession) => Unit]

  def run(engine: Engine, spark: SparkSession): Unit = {
    queries.foreach(queryFunction => queryFunction(engine, spark))
  }

}
