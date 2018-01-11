package tech.sourced.queryset

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import tech.sourced.engine._


//ISSUES:
//- Licenses
//- Repositories' main language
//- Join between two DataFrame coming from a GitRelation is not allowed by the GitOptimizer

// see https://github.com/src-d/engine/issues/218
object BoaQueries {

  def apply(spark: SparkSession): BoaQueries = new BoaQueries(spark)

}

class BoaQueries(spark: SparkSession) extends QueryExecutor with OutcomePrinter {

  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._

  private val RowsToShow: Int = 5

  override val HeadMessage: String = "- [Boa Query]"
  override val Colour: String = Console.GREEN_B

  override val queries: Seq[(Engine) => Unit] =
    ProgrammingLanguages.queries ++ ProjectManagement.queries ++ Legal.queries ++
      SourceCode.queries


  /*
  PROGRAMMING LANGUAGES
  1. What are the ten most used programming languages?
  2. How many projects use more than one programming language?
  3. How many projects use the Scheme programming language? (Gradle instead of Scheme)
  */

  private object ProgrammingLanguages extends QueryExecutor {

    override val queries: Seq[(Engine) => Unit] = Seq(
      mostUsedLanguages,
      projectUsingMoreThanXLanguages,
      projectsUsingXLanguage
    )

    // 1.
    def mostUsedLanguages(engine: Engine): Unit = {
      val NumOfLangs = 10
      val langsDf = engine
        .getRepositories
        .getReferences
        .getCommits
        .getFirstReferenceCommit
        .getBlobs
        .classifyLanguages
        .where("lang IS NOT NULL")
        .cache()

      val mostUsedLangsDf = langsDf.groupBy('lang).count().orderBy('count.desc).cache()
      printMessage(s"$NumOfLangs most used languages:")
      mostUsedLangsDf.show(NumOfLangs, false)
    }

    // 2.
    def projectUsingMoreThanXLanguages(engine: Engine): Unit = {
      val NumOfLangs = 1

      val langsDf = engine
        .getRepositories
        .getReferences
        .getCommits
        .getFirstReferenceCommit
        .getBlobs
        .classifyLanguages
        .cache()

      val projectLanguagesDf = langsDf
        .groupBy('repository_id)
        .agg(collect_set('lang) as "langs")
        .filter(size('langs) > NumOfLangs)
        .cache()

      val NumberOfProjects = projectLanguagesDf.count()
      printMessage(s"Projects using more than $NumOfLangs language: $NumberOfProjects")
      projectLanguagesDf.show(RowsToShow, false)
    }

    // 3.
    def projectsUsingXLanguage(engine: Engine): Unit = {
      val XLang = "Gradle"

      val langsDf = engine
        .getRepositories
        .getReferences
        .getCommits
        .getFirstReferenceCommit
        .getBlobs
        .classifyLanguages
        .where("lang IS NOT NULL")
        .cache()

      val projectsLangsDf = langsDf
        .groupBy('repository_id)
        .agg(collect_set('lang) as "langs")
        .filter(row => {
          val langs = row.getSeq(1)
          langs.contains(XLang)
        }).cache()

      val NumberOfProjects = projectsLangsDf.count()
      printMessage(s"Projects using language $XLang: $NumberOfProjects")
      projectsLangsDf.show(RowsToShow, false)
    }

  }

  /*
  PROJECT MANAGEMENT
  1. How many projects are created each year?
  !!! 2. How many projects self-classify into each topic provided by SourceForge?
  3. How many Java projects using SVN were active in 2011? (Git instead SVN, projects containing Java files)
  4. In which year was SVN added to Java projects the most? (Year when most of the Java projects were created)
  5. How many revisions are there in all Java projects using SVN? (Git,Commits in master branch, repos with Shell files)
  !!! 6. How many revisions fix bugs in all Java projects using SVN?
  7. How many committers are there for each project?
  !!! 8. What are the churn rates for all projects?
  9. How did the number of commits for Java projects using SVN change over years?
  !!! 10. How often are popular Java build systems used?
  */

  private object ProjectManagement extends QueryExecutor {

    override val queries: Seq[(Engine) => Unit] = Seq(
      projectsCreatedPerYear,
      projectsUsingXLangActivePerYear,
      yearMoreXLangProjectCreated,
      numberOfCommitsXLangProjects,
      committersPerProject,
      numberOfCommitsXLangProjectPerYear
    )

    // 1.
    def projectsCreatedPerYear(engine: Engine): Unit = {
      val commitsDf = engine.getRepositories.getReferences.getCommits.cache()

      printMessage(s"Projects created per year:")
      commitsDf
        .withColumn("year", year('committer_date))
        .groupBy('repository_id).min("year")
        .groupBy("min(year)").count
        .withColumnRenamed("min(year)", "year")
        .withColumnRenamed("count", "projects")
        .show(RowsToShow, false)
    }

    // 3. Git, instead SVN. Projects containing Java files
    def projectsUsingXLangActivePerYear(engine: Engine): Unit = {
      val XLang = "Java"
      val Year = 2015

      val commitsDf = engine.getRepositories.getReferences.getHEAD.getCommits

      val yearDf = commitsDf
        .withColumn("year", year('committer_date))
        .filter('year === Year)
        .groupBy('repository_id)
        .min("year")
        .withColumnRenamed("min(year)", "year")

      val langsDf = commitsDf.getBlobs.classifyLanguages

      val languageDf = langsDf
        .groupBy('repository_id)
        .pivot("lang", Seq(XLang))
        .count()
        .filter(col(XLang).isNotNull)
        .orderBy(desc(XLang))

      // !!! Note that two DataFrames coming from a GitRelation can't be joined
      // because of the GitOptimizer.
      //
      // Workaround: retrieve a list of the needed repositories and broadcast it.
      val repos: Seq[String] = getReposLang(spark, XLang, langsDf)
      val reposB: Broadcast[Seq[String]] = spark.sparkContext.broadcast(repos)

      val projectsDf = yearDf.filter('repository_id.isin(reposB.value: _*))
      val NumberOfProjects = projectsDf.count()

      printMessage(s"Projects using $XLang language active in $Year: $NumberOfProjects")
      projectsDf.show(RowsToShow, false)
    }

    // 4. Year when most of the Java projects were created
    def yearMoreXLangProjectCreated(engine: Engine): Unit = {
      val XLang = "Java"
      val langsDf = engine
        .getRepositories
        .getReferences
        .getMaster
        .getCommits
        .getBlobs
        .classifyLanguages
        .cache()

      // !!! Note that two DataFrames coming from a GitRelation can't be joined
      // because of the GitOptimizer.
      //
      // Workaround: retrieve a list of the needed repositories and broadcast it.
      val repos: Seq[String] = getReposLang(spark, XLang, langsDf)
      val reposB: Broadcast[Seq[String]] = spark.sparkContext.broadcast(repos)

      val commitsDf = engine
        .getRepositories
        .getReferences
        .getMaster
        .getCommits
        .filter('repository_id.isin(reposB.value: _*))
        .withColumn("year", year('committer_date))
        .cache()

      val numOfReposPerYearDf = commitsDf
        .groupBy("year")
        .agg(size(collect_set("repository_id")))
        .withColumnRenamed("size(collect_set(repository_id))", "created_repos_amount")
        .orderBy(desc("created_repos_amount"))
        .cache()

      val YearAmountPair: Row = numOfReposPerYearDf.first
      val Year = YearAmountPair.getInt(0)
      val Amount = YearAmountPair.getInt(1)
      printMessage(s"Year when more $XLang repositories were created: " +
        s"$Year with $Amount repositories")

      numOfReposPerYearDf.show(RowsToShow, false)
    }

    // 5. Git,Commits in master branch, repos with Shell files
    def numberOfCommitsXLangProjects(engine: Engine): Unit = {
      val XLang = "Shell"
      val langsDf = engine
        .getRepositories
        .getReferences
        .getMaster
        .getCommits
        .getBlobs
        .classifyLanguages

      // !!! Note that two DataFrames coming from a GitRelation can't be joined
      // because of the GitOptimizer.
      //
      // Workaround: retrieve a list of the needed repositories and broadcast it.
      val repos: Seq[String] = getReposLang(spark, XLang, langsDf)
      val reposB: Broadcast[Seq[String]] = spark.sparkContext.broadcast(repos)

      val projectsDf = langsDf
        .groupBy('repository_id)
        .agg(size(collect_set("commit_hash")))
        .withColumnRenamed("size(collect_set(commit_hash))", "commits_amount")
        .filter('repository_id.isin(reposB.value: _*))
        .cache()

      val NumberOfCommits = projectsDf.agg(sum("commits_amount")).first().getLong(0)
      printMessage(s"Number of commits from projects using $XLang language: $NumberOfCommits")
      projectsDf.show(RowsToShow, false)
    }

    // 7.
    def committersPerProject(engine: Engine): Unit = {
      val commitsDf = engine.getRepositories.getReferences.getCommits.cache()
      printMessage("Number of committers per project:")
      commitsDf
        .groupBy('repository_id)
        .agg(size(collect_set('author_name)) as "committers")
        .orderBy(desc("committers"))
        .show(RowsToShow, false)
    }

    // 9. Number of commits in master branch per year for projects which contains XLang files.
    def numberOfCommitsXLangProjectPerYear(engine: Engine): Unit = {
      val XLang = "Java"
      val langsDf = engine
        .getRepositories
        .getReferences
        .getMaster
        .getCommits
        .getBlobs
        .classifyLanguages
        .cache()

      // !!! Note that two DataFrames coming from a GitRelation can't be joined
      // because of the GitOptimizer.
      //
      // Workaround: retrieve a list of the needed repositories and broadcast it.
      val repos: Seq[String] = getReposLang(spark, XLang, langsDf)
      val reposB: Broadcast[Seq[String]] = spark.sparkContext.broadcast(repos)

      val commitsDf = engine
        .getRepositories
        .getReferences
        .getMaster
        .getCommits
        .withColumn("year", year('committer_date))
        .cache()

      val commitsPerYearDf = commitsDf
        .groupBy('repository_id)
        .pivot("year")
        .agg(size(collect_set("hash")))
        .cache()

      val reposDf = commitsPerYearDf
        .filter('repository_id.isin(reposB.value: _*))
        .cache()

      val nullCols: Array[String] = reposDf.columns.filter(column =>
        reposDf.filter(reposDf(column).isNull).count() == reposDf.count()
      )

      printMessage(s"Number of commits per year for projects using $XLang language:")
      reposDf.drop(nullCols: _*).show(RowsToShow, false)
    }

    private def getReposLang(spark: SparkSession,
                             lang: String,
                             langsDf: DataFrame): Seq[String] = {

      val repos: Seq[String] = langsDf
        .filter('lang === lang)
        .select('repository_id)
        .distinct()
        .map(r => r.getString(0))
        .collect()
        .toList

      repos
    }
  }

  /*
  LEGAL
  1. What are the five most used licenses?
  2. How many projects use more than one license?
  */

  private object Legal extends QueryExecutor {

    override val queries: Seq[(Engine) => Unit] = Seq(
      mostUsedLicenses,
      projectsUsingMoreThanOneLicense
    )

    //1.
    def mostUsedLicenses(engine: Engine): Unit = {
      val blobsDf = engine
        .getRepositories
        .getReferences
        .getMaster
        .getCommits
        .getFirstReferenceCommit
        .getBlobs
        .cache()

      val licenseDf = blobsDf
        .filter('path.contains("LICENSE") || 'path.contains("license") || 'path.contains("License"))
        .withColumn("license", identifyLicense('content))
        .cache()

      printMessage("Most used licenses:")
      licenseDf
        .groupBy('license)
        .count()
        .orderBy('count)
        .show(false)

      // to debug
      // licenseDf
      //  .select('repository_id, 'path, 'license)
      //  .show(56, false)
    }

    //2.
    def projectsUsingMoreThanOneLicense(engine: Engine): Unit = {
      val blobsDf = engine
        .getRepositories
        .getReferences
        .getMaster
        .getCommits
        .getFirstReferenceCommit
        .getBlobs
        .cache()

      val licenseDf = blobsDf
        .filter('path.contains("LICENSE") || 'path.contains("license") || 'path.contains("License"))
        .withColumn("license", identifyLicense('content))
        .cache()

      val projectLicensesDf = licenseDf
        .groupBy('repository_id)
        .agg(collect_set('license) as "licenses")
        .filter(size('licenses) > 1)
        .cache()

      val NumberOfProjects = projectLicensesDf.count()
      printMessage(s"Projects using more than one license: $NumberOfProjects")
      projectLicensesDf.show(false)
    }

  }

  /*
  !!! PLATFORM/ENVIRONMENT
  1. What are the five most supported operating systems?
  2. Which projects support multiple operating systems?
  3. What are the five most popular databases?
  4. What are the projects that support multiple databases?
  5. How often is each database used in each programming language?
  */

  private object PlatformEnvironment extends QueryExecutor {

    override val queries: Seq[(Engine) => Unit] = Seq(
      x
    )

    def x(engine: Engine): Unit = ???

  }

  /*
  SOURCE CODE
  1. What are the five largest projects, in terms of AST nodes?
  !!! 2. How many valid Java files in latest snapshot?
  !!! 3. How many fixing revisions added null checks?
  !!! 4. What files have unreachable statements?
  !!! 5. How many generic fields are declared in each project?
  !!! 6. How is varargs used over time?
  !!! 7. How is transient keyword used in Java?
  */

  private object SourceCode extends QueryExecutor {

    override val queries: Seq[(Engine) => Unit] = Seq(
      largestProjectsPerASTNodes
    )

    // 1.
    def largestProjectsPerASTNodes(engine: Engine): Unit = {
      val blobsDf = engine
        .getRepositories
        .getMaster
        .getCommits
        .getFirstReferenceCommit
        .getBlobs
        .classifyLanguages
        .where("lang='Python' OR lang='Java'")
        .extractUASTs()
        .cache()

      val reposDf = blobsDf
        .filter(size('uast) > 0)
        .queryUAST("//*", "uast", "result")
        .withColumn("uast_nodes", size('result))
        .groupBy('repository_id)
        .sum("uast_nodes")
        .withColumnRenamed("sum(uast_nodes)", "uast_nodes")
        .orderBy('uast_nodes.desc)

      val NumOfRepos = 5
      printMessage(s"$NumOfRepos largest projects in terms of AST nodes:")
      reposDf.show(NumOfRepos, false)
    }

  }

  /*
  !!! SOFTWARE ENGINEERING METRICS
  1. What are the number of attributes (NOA), per-project and per-type?
  2. What are the number of public methods (NPM), per-project and per-type?
  */

  private object SoftwareEngineeringMetrics extends QueryExecutor {

    override val queries: Seq[(Engine) => Unit] = Seq(
      x
    )

    def x(engine: Engine): Unit = ???

  }

}
