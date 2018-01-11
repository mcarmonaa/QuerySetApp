package tech.sourced.queryset

import org.apache.spark.sql.SparkSession
import tech.sourced.engine._


object SourcedQueries {

  def apply(spark: SparkSession): SourcedQueries = new SourcedQueries(spark)

}

class SourcedQueries(spark: SparkSession) extends QueryExecutor with OutcomePrinter {

  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._

  private val RowsToShow: Int = 5

  override val HeadMessage: String = "- [Sourced Query]"
  override val Colour: String = Console.BLUE_B

  override val queries: Seq[(Engine) => Unit] = Seq(
    extractCommitsFromHead,
    filterCommitsFromHead,
    blobsPerCommit,
    lastBlobsFromNoForks,
    methodsPerPythonBlob,
    tokensUsedAsParametersInPythonFunctionCalls,
    tokensUsedAsArgumentsInPythonFunctionDeclarations,
    argumentsWithDefaultValuePerPythonBlob,
    lambdaFunctionsPerPythonBlob,
    privateMethodDeclarationPerJavaBlob,
    commentedLinesPerJavaBlob,
    commitsPerMonthPerUserPerRepoInYear,
    commitsPointedByMoreThanXReferencesPerRepo
  )

  private def extractCommitsFromHead(engine: Engine): Unit = {
    val commitsDf = engine.getRepositories.getReferences.getHEAD.getCommits
    printMessage("Extract commits from HEAD reference:")
    commitsDf.show(RowsToShow)
  }

  private def filterCommitsFromHead(engine: Engine): Unit = {
    val filteredCommitsDf = engine
      .getRepositories
      .getReferences
      .getHEAD
      .getCommits
      .where("index <= 4")

    printMessage("Extract and filter commits by index from HEAD reference:")
    filteredCommitsDf.show(RowsToShow)
  }

  private def blobsPerCommit(engine: Engine): Unit = {
    val treesDf = engine
      .getRepositories
      .getReferences
      .getMaster
      .getCommits
      .getTreeEntries
      .cache()

    val blobCountDf = treesDf
      .groupBy('repository_id, 'commit_hash)
      .agg(size(collect_set('blob)))
      .withColumnRenamed("size(collect_set(blob))", "blob_amount")

    printMessage("Number of blobs per commit per repository:")
    blobCountDf.show(RowsToShow, false)
  }

  private def lastBlobsFromNoForks(engine: Engine): Unit = {
    val blobsDf = engine
      .getRepositories
      .filter('is_fork === false)
      .getMaster
      .getCommits
      .getFirstReferenceCommit
      .getBlobs
      .cache()

    printMessage("Retrieving all the files at latest commit of the main branch of non-forks:")
    blobsDf.show(RowsToShow)
  }

  private def methodsPerPythonBlob(engine: Engine): Unit = {
    val Lang = "Python"

    val blobsDf = engine
      .getRepositories
      .getHEAD
      .getCommits
      .getFirstReferenceCommit
      .getBlobs
      .dropDuplicates("blob_id")
      .classifyLanguages
      .where(s"lang='${Lang}'")
      .extractUASTs
      .cache()

    val methodsDf = blobsDf
      .queryUAST("//ClassDef.body/*[@roleFunction and @roleDeclaration and @roleIdentifier]")
      .extractTokens()
      .filter(size('result) > 0)
      .select('repository_id, 'blob_id, 'path, 'tokens)
      .withColumn("method_amount", size('tokens))

    printMessage(s"Number of methods per ${Lang} blob:")
    methodsDf.show(RowsToShow, false)
  }

  private def tokensUsedAsParametersInPythonFunctionCalls(engine: Engine): Unit = {
    val Lang = "Python"

    val blobsDf = engine
      .getRepositories
      .getHEAD
      .getCommits
      .getFirstReferenceCommit
      .getBlobs
      .dropDuplicates("blob_id")
      .classifyLanguages
      .where(s"lang='${Lang}'")
      .extractUASTs
      .cache()

    val tokensDf = blobsDf
      .queryUAST("//*[@roleFunction and @roleCall]/*[@roleArgument]")
      .extractTokens()
      .filter(size('result) > 0)
      .select('repository_id, 'blob_id, 'path, 'tokens)

    printMessage(s"Tokens used as parameters in functions calls per ${Lang} blob:")
    tokensDf.show(RowsToShow, false)
  }

  private def tokensUsedAsArgumentsInPythonFunctionDeclarations(engine: Engine): Unit = {
    val Lang = "Python"

    val blobsDf = engine
      .getRepositories
      .getHEAD
      .getCommits
      .getFirstReferenceCommit
      .getBlobs
      .dropDuplicates("blob_id")
      .classifyLanguages
      .where(s"lang='${Lang}'")
      .extractUASTs
      .cache()

    val tokensDf = blobsDf
      .queryUAST("//FunctionDef//*[@roleArgument and @roleIdentifier]")
      .extractTokens()
      .filter(size('result) > 0)
      .select('repository_id, 'blob_id, 'path, 'tokens)

    printMessage(s"Tokens used as arguments in function declarations per ${Lang} blob:")
    tokensDf.show(RowsToShow, false)
  }

  private def argumentsWithDefaultValuePerPythonBlob(engine: Engine): Unit = {
    val Lang = "Python"

    val blobsDf = engine
      .getRepositories
      .getHEAD
      .getCommits
      .getFirstReferenceCommit
      .getBlobs
      .dropDuplicates("blob_id")
      .classifyLanguages
      .where(s"lang='${Lang}'")
      .extractUASTs
      .cache()

    val argsDf = blobsDf
      .queryUAST("//FunctionDef//arguments.defaults/*[@roleLiteral]")
      .extractTokens()
      .filter(size('result) > 0)
      .withColumn("num_default_args", size('tokens))
      .select('repository_id, 'blob_id, 'path, 'num_default_args)

    printMessage(s"Number of default arguments in function declarations per ${Lang} blob:")
    argsDf.show(RowsToShow, false)
  }

  private def lambdaFunctionsPerPythonBlob(engine: Engine): Unit = {
    val Lang = "Python"

    val blobsDf = engine
      .getRepositories
      .getHEAD
      .getCommits
      .getFirstReferenceCommit
      .getBlobs
      .dropDuplicates("blob_id")
      .classifyLanguages
      .where(s"lang='${Lang}'")
      .extractUASTs
      .cache()

    val lambdaDf = blobsDf
      .queryUAST("//Lambda[@roleFunction and @roleDeclaration]")
      .extractTokens()
      .filter(size('result) > 0)
      .withColumn("lambda_functions", size('tokens))
      .select('repository_id, 'blob_id, 'path, 'lambda_functions)
      .orderBy('lambda_functions.desc)

    printMessage(s"Number of lambda functions per ${Lang} blob:")
    lambdaDf.show(RowsToShow, false)
  }

  private def privateMethodDeclarationPerJavaBlob(engine: Engine): Unit = {
    val Lang = "Java"

    val blobsDf = engine
      .getRepositories
      .getHEAD
      .getCommits
      .getFirstReferenceCommit
      .getBlobs
      .dropDuplicates("blob_id")
      .classifyLanguages
      .where(s"lang='${Lang}'")
      .extractUASTs
      .cache()

    val lambdaDf = blobsDf
      .queryUAST("//MethodDeclaration/Modifier[@token='private']")
      .extractTokens()
      .filter(size('result) > 0)
      .withColumn("private_methods", size('tokens))
      .select('repository_id, 'blob_id, 'path, 'private_methods)
      .orderBy('private_methods.desc)

    printMessage(s"Number of private methods per ${Lang} blob:")
    lambdaDf.show(RowsToShow, false)
  }

  private def commentedLinesPerJavaBlob(engine: Engine): Unit = {
    val Lang = "Java"

    val blobsDf = engine
      .getRepositories
      .getHEAD
      .getCommits
      .getFirstReferenceCommit
      .getBlobs
      .dropDuplicates("blob_id")
      .classifyLanguages
      .where(s"lang='${Lang}'")
      .extractUASTs
      .cache()

    val lambdaDf = blobsDf
      .queryUAST("//LineComment[@roleComment]")
      .extractTokens()
      .filter(size('result) > 0)
      .withColumn("commented_lines", size('tokens))
      .select('repository_id, 'blob_id, 'path, 'commented_lines)
      .orderBy('commented_lines.desc)

    printMessage(s"Number of commented lines per ${Lang} blob:")
    lambdaDf.show(RowsToShow, false)
  }

  private def commitsPerMonthPerUserPerRepoInYear(engine: Engine): Unit = {
    val Year = 2017
    val Months = 1 to 12 toList

    val commitsDf = engine
      .getRepositories
      .getReferences
      .getCommits
      .dropDuplicates("hash")
      .filter(year('committer_date) === Year)
      .withColumn("month", month('committer_date))
      .groupBy('repository_id, 'committer_email)
      .pivot("month", Months)
      .agg(count('hash))

    printMessage(s"Number of commits per committer per month and per repository in year $Year:")
    commitsDf.show(RowsToShow, false)
  }

  private def commitsPointedByMoreThanXReferencesPerRepo(engine: Engine): Unit = {
    val NumRefs = 1

    val commitsDf = engine
      .getRepositories
      .getReferences
      .groupBy('repository_id, 'hash)
      .agg(count('name))
      .withColumnRenamed("count(name)", "names")
      .filter('names > 1)

    printMessage(s"Number of commits pointed by more than $NumRefs per repository:")
    commitsDf.show(RowsToShow, false)
  }

}
