package tech.sourced.queryset

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import tech.sourced.engine._

object SourcedQueries extends QueryExecutor with OutcomePrinter {

  override val HeadMessage: String = "- [Sourced Query]"
  override val Colour: String = Console.BLUE_B

  private val RowsToShow: Int = 5

  override val queries: Seq[(Engine, SparkSession) => Unit] = Seq(
    extractCommitsFromHead,
    filterCommitsFromHead
  )

  private def extractCommitsFromHead(engine: Engine, spark: SparkSession): Unit = {
    val commitsDf: DataFrame = engine.getRepositories.getReferences.getHEAD.getCommits
    printMessage("Extract commits from HEAD reference:")
    commitsDf.show(RowsToShow)
  }

  private def filterCommitsFromHead(engine: Engine, spark: SparkSession): Unit = {
    val filteredCommitsDf: Dataset[Row] = engine
      .getRepositories
      .getReferences
      .getHEAD
      .getCommits
      .where("index <= 4")

    printMessage("Extract and filter commits by index from HEAD reference:")
    filteredCommitsDf.show(RowsToShow)
  }

}
