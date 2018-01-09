package tech.sourced.queryset

import org.apache.spark.sql.SparkSession
import tech.sourced.engine._


object SourcedQueries {

  def apply(spark: SparkSession): SourcedQueries = new SourcedQueries(spark)

  private val RowsToShow: Int = 5

}

class SourcedQueries(spark: SparkSession) extends QueryExecutor with OutcomePrinter {

  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._

  override val HeadMessage: String = "- [Sourced Query]"
  override val Colour: String = Console.BLUE_B

  override val queries: Seq[(Engine) => Unit] = Seq(
    extractCommitsFromHead,
    filterCommitsFromHead,
    blobsPerCommit
  )

  private def extractCommitsFromHead(engine: Engine): Unit = {
    val commitsDf = engine.getRepositories.getReferences.getHEAD.getCommits
    printMessage("Extract commits from HEAD reference:")
    commitsDf.show(SourcedQueries.RowsToShow)
  }

  private def filterCommitsFromHead(engine: Engine): Unit = {
    val filteredCommitsDf = engine
      .getRepositories
      .getReferences
      .getHEAD
      .getCommits
      .where("index <= 4")

    printMessage("Extract and filter commits by index from HEAD reference:")
    filteredCommitsDf.show(SourcedQueries.RowsToShow)
  }

  private def blobsPerCommit(engine: Engine): Unit = {
    val treesDf = engine
      .getRepositories
      .getReferences
      .getMaster
      .getCommits
      .getTreeEntries
      .cache

    val blobCountDf = treesDf
      .groupBy('repository_id, 'commit_hash)
      .agg(size(collect_set('blob)))
      .withColumnRenamed("size(collect_set(blob))", "blob_amount")

    printMessage("Number of blobs per commit per repository:")
    blobCountDf.show(SourcedQueries.RowsToShow, false)
  }

}
