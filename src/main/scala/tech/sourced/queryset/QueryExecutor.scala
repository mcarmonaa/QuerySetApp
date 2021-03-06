package tech.sourced.queryset

import tech.sourced.engine.Engine


trait QueryExecutor {

  def engine: Engine

  def queries: Seq[(Engine) => Unit]

  def run(): Unit = {
    queries.foreach(queryFunction => queryFunction(engine))
  }

}
