package tech.sourced.queryset

import tech.sourced.engine.Engine


trait QueryExecutor {

  def queries: Seq[(Engine) => Unit]

  def run(engine: Engine): Unit = {
    queries.foreach(queryFunction => queryFunction(engine))
  }

}
