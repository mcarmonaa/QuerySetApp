package tech.sourced.queryset

import org.apache.spark.sql.{DataFrame, SparkSession}
import tech.sourced.engine._

object BoaQueries extends QueryExecutor with OutcomePrinter {
  /* see https://github.com/src-d/engine/issues/218 */

  private val RowsToShow: Int = 5

  override val HeadMessage: String = "- [Boa Query]"
  override val Colour: String = Console.GREEN_B

  override val queries: Seq[(Engine, SparkSession) => Unit] =
    ProgrammingLanguages.queries ++ ProjectManagement.queries ++ Legal.queries

  // ProgrammingLanguages.queries ++ ProjectManagement.queries ++ Legal.queries ++
  // PlatformEnvironment.queries ++ SourceCode.queries ++ SoftwareEngineeringMetrics.queries

  /*
  PROGRAMMING LANGUAGES
  1. What are the ten most used programming languages?
  2. How many projects use more than one programming language?
  3. How many projects use the Scheme programming language?
  */

  private object ProgrammingLanguages extends QueryExecutor {

    override val queries: Seq[(Engine, SparkSession) => Unit] = Seq(
      mostUsedLanguages,
      projectUsingMoreThanXLanguages,
      projectsUsingXLanguage
    )

    def mostUsedLanguages(engine: Engine, spark: SparkSession): Unit = {
      import spark.sqlContext.implicits._

      val NumOfLangs: Int = 10
      val langsDf: DataFrame = engine
        .getRepositories
        .getReferences
        .getCommits
        .getFirstReferenceCommit
        .getBlobs
        .classifyLanguages
        .cache

      val mostUsedLangsDf = langsDf.groupBy('lang).count.orderBy('count.desc).cache
      printMessage(s"$NumOfLangs most used languages:")
      mostUsedLangsDf.show(NumOfLangs, false)
    }

    def projectUsingMoreThanXLanguages(engine: Engine, spark: SparkSession): Unit = {
      import org.apache.spark.sql.functions._
      import spark.sqlContext.implicits._

      val NumOfLangs = 1

      val langsDf: DataFrame = engine
        .getRepositories
        .getReferences
        .getCommits
        .getFirstReferenceCommit
        .getBlobs
        .classifyLanguages
        .cache

      val projectLanguagesDf = langsDf
        .groupBy('repository_id)
        .agg(collect_set('lang) as "langs")
        .filter(size('langs) > NumOfLangs)
        .cache

      val NumberOfProjects = projectLanguagesDf.count
      printMessage(s"Projects using more than $NumOfLangs language: $NumberOfProjects")
      projectLanguagesDf.show(RowsToShow, false)
    }

    def projectsUsingXLanguage(engine: Engine, spark: SparkSession): Unit = {
      import org.apache.spark.sql.functions._
      import spark.sqlContext.implicits._

      val XLang: String = "Scheme"

      val langsDf: DataFrame = engine
        .getRepositories
        .getReferences
        .getCommits
        .getFirstReferenceCommit
        .getBlobs
        .classifyLanguages
        .cache

      val projectsLangsDf = langsDf
        .groupBy('repository_id)
        .agg(collect_set('lang) as "langs")
        .filter(row => {
          val langs = row.getSeq(1)
          langs.contains(XLang)
        }).cache

      val NumberOfProjects = projectsLangsDf.count
      printMessage(s"Projects using language $XLang: $NumberOfProjects")
      projectsLangsDf.show(RowsToShow, false)
    }

  }

  /*
  PROJECT MANAGEMENT
  1. How many projects are created each year?
  !!! 2. How many projects self-classify into each topic provided by SourceForge?

  --- NEED FILTER BY LANGUAGE WORKING PROPERLY
  !!! 3. How many Java projects using SVN were active in 2011? (Git instead SVN, projects containing Java files)
  !!! 4. In which year was SVN added to Java projects the most?
  !!! 5. How many revisions are there in all Java projects using SVN?
  !!! 6. How many revisions fix bugs in all Java projects using SVN?
  ---

  7. How many committers are there for each project?
  !!! 8. What are the churn rates for all projects?

  --- NEED FILTER BY LANGUAGE WORKING PROPERLY
  !!! 9. How did the number of commits for Java projects using SVN change over years?
  !!! 10. How often are popular Java build systems used?
  ---
  */

  private object ProjectManagement extends QueryExecutor {

    override val queries: Seq[(Engine, SparkSession) => Unit] = Seq(
      projectsCreatedPerYear,
      committersPerProject
    )

    def projectsCreatedPerYear(engine: Engine, spark: SparkSession): Unit = {
      import org.apache.spark.sql.functions._
      import spark.sqlContext.implicits._

      val commitsDf = engine.getRepositories.getReferences.getCommits.cache

      printMessage(s"Projects created per year:")
      commitsDf
        .withColumn("year", year('committer_date))
        .groupBy('repository_id).min("year")
        .groupBy("min(year)").count
        .withColumnRenamed("min(year)", "year")
        .withColumnRenamed("count", "projects")
        .show(RowsToShow, false)
    }

    def committersPerProject(engine: Engine, spark: SparkSession): Unit = {
      import org.apache.spark.sql.functions._
      import spark.sqlContext.implicits._

      val commitsDf = engine.getRepositories.getReferences.getCommits.cache
      printMessage("Number of committers per project:")
      commitsDf
        .groupBy('repository_id)
        .agg(size(collect_set('author_name)) as "committers")
        .show(RowsToShow, false)
    }

  }

  /*
  LEGAL
  1. What are the five most used licenses?
  2. How many projects use more than one license?
  */

  private object Legal extends QueryExecutor {

    override val queries: Seq[(Engine, SparkSession) => Unit] = Seq(
      mostUsedLicenses,
      projectsUsingMoreThanOneLicense
    )

    def mostUsedLicenses(engine: Engine, spark: SparkSession): Unit = {
      // import org.apache.spark.sql.functions._
      import spark.sqlContext.implicits._

      val blobsDf = engine
        .getRepositories
        .getReferences
        .getMaster
        .getCommits
        .getFirstReferenceCommit
        .getBlobs
        .cache

      val licenseDf = blobsDf
        .filter('path.contains("LICENSE") || 'path.contains("license") || 'path.contains("License"))
        .withColumn("license", identifyLicense('content))
        .cache

      printMessage("Most used licenses:")
      licenseDf
        .groupBy('license)
        .count
        .orderBy('count)
        .show(false)

      // to debug
      // licenseDf
      //  .select('repository_id, 'path, 'license)
      //  .show(56, false)
    }

    def projectsUsingMoreThanOneLicense(engine: Engine, spark: SparkSession): Unit = {
      import org.apache.spark.sql.functions._
      import spark.sqlContext.implicits._

      val blobsDf = engine
        .getRepositories
        .getReferences
        .getMaster
        .getCommits
        .getFirstReferenceCommit
        .getBlobs
        .cache

      val licenseDf = blobsDf
        .filter('path.contains("LICENSE") || 'path.contains("license") || 'path.contains("License"))
        .withColumn("license", identifyLicense('content))
        .cache

      val projectLicensesDf = licenseDf
        .groupBy('repository_id)
        .agg(collect_set('license) as "licenses")
        .filter(size('licenses) > 1)
        .cache

      val NumberOfProjects = projectLicensesDf.count
      printMessage(s"Projects using more than one license: $NumberOfProjects")
      projectLicensesDf.show(false)
    }

  }

  /*
  PLATFORM/ENVIRONMENT
  1. What are the five most supported operating systems?
  2. Which projects support multiple operating systems?
  3. What are the five most popular databases?
  4. What are the projects that support multiple databases?
  5. How often is each database used in each programming language?
  */

  private object PlatformEnvironment extends QueryExecutor {

    override val queries: Seq[(Engine, SparkSession) => Unit] = Seq(
      mostSupportedOs,
      projectsSupportingSeveralOs,
      mostPopularDb,
      projectsSupportingSeveralDb,
      dbFrequencyPerLanguage
    )

    def mostSupportedOs(engine: Engine, spark: SparkSession): Unit = ???

    def projectsSupportingSeveralOs(engine: Engine, spark: SparkSession): Unit = ???

    def mostPopularDb(engine: Engine, spark: SparkSession): Unit = ???

    def projectsSupportingSeveralDb(engine: Engine, spark: SparkSession): Unit = ???

    def dbFrequencyPerLanguage(engine: Engine, spark: SparkSession): Unit = ???

  }

  /*
  SOURCE CODE
  1. What are the five largest projects, in terms of AST nodes?
  2. How many valid Java files in latest snapshot?
  !!! 3. How many fixing revisions added null checks?
  4. What files have unreachable statements?
  5. How many generic fields are declared in each project?
  6. How is varargs used over time?
  7. How is transient keyword used in Java?
  */

  private object SourceCode extends QueryExecutor {

    override val queries: Seq[(Engine, SparkSession) => Unit] = Seq(
      mostLargestProjectsByAst,
      validLangFilesInLatestSnapshots,
      filesWithUnreachableStatements,
      genericFieldsPerProject,
      varArgsUsed,
      transientKeywordUsage
    )

    def mostLargestProjectsByAst(engine: Engine, spark: SparkSession): Unit = ???

    def validLangFilesInLatestSnapshots(engine: Engine, spark: SparkSession): Unit = ???

    def filesWithUnreachableStatements(engine: Engine, spark: SparkSession): Unit = ???

    def genericFieldsPerProject(engine: Engine, spark: SparkSession): Unit = ???

    def varArgsUsed(engine: Engine, spark: SparkSession): Unit = ???

    def transientKeywordUsage(engine: Engine, spark: SparkSession): Unit = ???

  }

  /*
  SOFTWARE ENGINEERING METRICS
  !!! 1. What are the number of attributes (NOA), per-project and per-type?
  !!! 2. What are the number of public methods (NPM), per-project and per-type?
  */

  private object SoftwareEngineeringMetrics extends QueryExecutor {

    override val queries: Seq[(Engine, SparkSession) => Unit] = Seq(
      x
    )

    def x(engine: Engine, spark: SparkSession): Unit = ???

  }

}
