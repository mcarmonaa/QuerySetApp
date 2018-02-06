package tech.sourced.queryset

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import tech.sourced.engine._

import scala.collection.mutable


// see https://github.com/src-d/engine/issues/218
object BoaQueries {

  def apply(engine: Engine): BoaQueries = new BoaQueries(engine)

}

class BoaQueries(override val engine: Engine) extends QueryExecutor with OutcomePrinter {

  import engine.session.sqlContext.implicits._
  import org.apache.spark.sql.functions._

  private val RowsToShow: Int = 5

  // OutcomePrinter
  override val HeadMessage: String = "- [Boa Query]"
  override val Colour: String = Console.GREEN_B

  // QueryExecutor
  override val queries: Seq[(Engine) => Unit] =
    ProgrammingLanguages.queries ++ ProjectManagement.queries ++ Legal.queries ++
      PlatformEnvironment.queries ++ SourceCode.queries ++ SoftwareEngineeringMetrics.queries

  private val commitsDf = engine
    .getRepositories
    .filter(size('urls) > 0)
    .getMaster
    .getCommits

  private val blobsDf = commitsDf
    .getBlobs
    .classifyLanguages

  /*
  PROGRAMMING LANGUAGES
  1. What are the ten most used programming languages?
  2. How many projects use more than one programming language?
  3. How many projects use the Scheme programming language? (Gradle instead of Scheme)
  */

  private object ProgrammingLanguages {

    val queries: Seq[(Engine) => Unit] = Seq(
      mostUsedLanguages,
      projectUsingMoreThanOneLanguages,
      projectsUsingALanguage
    )

    // 1.
    def mostUsedLanguages(engine: Engine): Unit = {
      val NumberOfLangs = 10

      val mostUsedLangsDf = blobsDf
        .where("lang IS NOT NULL")
        .groupBy('lang)
        .count()
        .orderBy('count.desc)
        .cache()

      printMessage(s"$NumberOfLangs most used languages:")
      mostUsedLangsDf.show(NumberOfLangs, false)
    }

    // 2.
    def projectUsingMoreThanOneLanguages(engine: Engine): Unit = {
      val NumberOfLangs = 1

      val projectLanguagesDf = blobsDf
        .groupBy('repository_id)
        .agg(collect_set('lang) as "langs")
        .filter(size('langs) > NumberOfLangs)

      val NumberOfProjects = projectLanguagesDf.count()
      printMessage(s"Projects using more than $NumberOfLangs language: $NumberOfProjects")
      projectLanguagesDf.show(RowsToShow, false)
    }

    // 3.
    def projectsUsingALanguage(engine: Engine): Unit = {
      val Language = "Gradle"

      val projectsLangsDf = blobsDf
        .where("lang IS NOT NULL")
        .groupBy('repository_id)
        .agg(collect_set('lang) as "langs")
        .filter(row => {
          val langs = row.getSeq(1)
          langs.contains(Language)
        })

      val NumberOfProjects = projectsLangsDf.count()
      printMessage(s"Projects using language $Language: $NumberOfProjects")
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
  6. How many revisions fix bugs in all Java projects using SVN? (commits containing "fix" in their messages)
  7. How many committers are there for each project?
  !!! 8. What are the churn rates for all projects?
  9. How did the number of commits for Java projects using SVN change over years?
  10. How often are popular Java build systems used?
  */

  private object ProjectManagement {

    val queries: Seq[(Engine) => Unit] = Seq(
      projectsCreatedPerYear,
      projectsUsingALangActivePerYear,
      yearMoreLangProjectCreated,
      numberOfCommitsLangProjects,
      commitsFixingBugs,
      committersPerProject,
      numberOfCommitsXLangProjectPerYear,
      javaBuildSystems
    )

    // 1.
    def projectsCreatedPerYear(engine: Engine): Unit = {
      printMessage(s"Projects created per year:")
      commitsDf
        .getAllReferenceCommits
        .withColumn("year", year('committer_date))
        .groupBy('repository_id).min("year")
        .groupBy("min(year)").count
        .withColumnRenamed("min(year)", "year")
        .withColumnRenamed("count", "projects")
        .show(RowsToShow, false)
    }

    // 3. Git, instead SVN. Projects containing Java files
    def projectsUsingALangActivePerYear(engine: Engine): Unit = {
      val Language = "Java"
      val Year = 2015

      val yearDf = commitsDf
        .getAllReferenceCommits
        .withColumn("year", year('committer_date))
        .filter('year === Year)
        .groupBy('repository_id)
        .min("year")
        .withColumnRenamed("min(year)", "year")

      val langsDf = commitsDf.getBlobs.classifyLanguages

      val languageDf = langsDf
        .groupBy('repository_id)
        .pivot("lang", Seq(Language))
        .count()
        .filter(col(Language).isNotNull)
        .orderBy(desc(Language))

      val repos: Seq[String] = getReposLang(Language, langsDf)
      val reposB: Broadcast[Seq[String]] = engine.session.sparkContext.broadcast(repos)

      val projectsDf = yearDf.filter('repository_id.isin(reposB.value: _*))
      val NumberOfProjects = projectsDf.count()

      printMessage(s"Projects using $Language language active in $Year: $NumberOfProjects")
      projectsDf.show(RowsToShow, false)
    }

    // 4. Year when most of the Java projects were created
    def yearMoreLangProjectCreated(engine: Engine): Unit = {
      val Language = "Java"
      val langsDf = commitsDf
        .getBlobs
        .classifyLanguages

      val repos: Seq[String] = getReposLang(Language, langsDf)
      val reposB: Broadcast[Seq[String]] = engine.session.sparkContext.broadcast(repos)

      val yearDf = commitsDf
        .getAllReferenceCommits
        .filter('repository_id.isin(reposB.value: _*))
        .withColumn("year", year('committer_date))

      val numOfReposPerYearDf = yearDf
        .groupBy("year")
        .agg(size(collect_set("repository_id")))
        .withColumnRenamed("size(collect_set(repository_id))", "created_repos_amount")
        .orderBy(desc("created_repos_amount"))
        .cache()

      val YearAmountPair: Row = numOfReposPerYearDf.first
      val Year = YearAmountPair.getInt(0)
      val Amount = YearAmountPair.getInt(1)
      printMessage(s"Year when more $Language repositories were created: " +
        s"$Year with $Amount repositories")

      numOfReposPerYearDf.show(RowsToShow, false)
    }

    // 5. Git,Commits in master branch, repos with Shell files
    def numberOfCommitsLangProjects(engine: Engine): Unit = {
      val Language = "Shell"
      val langsDf = commitsDf
        .getAllReferenceCommits
        .getBlobs
        .classifyLanguages

      val repos: Seq[String] = getReposLang(Language, langsDf)
      val reposB: Broadcast[Seq[String]] = engine.session.sparkContext.broadcast(repos)

      val projectsDf = langsDf
        .groupBy('repository_id)
        .agg(size(collect_set("commit_hash")))
        .withColumnRenamed("size(collect_set(commit_hash))", "commits_amount")
        .filter('repository_id.isin(reposB.value: _*))
        .cache()

      val NumberOfCommits = if (projectsDf.take(1).nonEmpty) {
        projectsDf.agg(sum("commits_amount")).first().getLong(0)
      } else {
        0L
      }

      printMessage(s"Number of commits from projects using $Language language: $NumberOfCommits")
      projectsDf.show(RowsToShow, false)
    }

    // 6.
    def commitsFixingBugs(engine: Engine): Unit = {
      val Language = "Java"

      val fixDf = commitsDf
        .filter('message.contains("fix") || 'message.contains("Fix") || 'message.contains("FIX"))

      val commits: Seq[String] = commitsDf
        .getAllReferenceCommits
        .getBlobs
        .classifyLanguages
        .where(s"lang='${Language}'")
        .select('commit_hash)
        .distinct()
        .map(_.getString(0))
        .collect()
        .toList

      val commitsB = engine.session.sparkContext.broadcast(commits)
      val commitsFixDf = fixDf.filter('hash.isin(commitsB.value: _*))

      val NumberOfCommits = commitsFixDf.count()
      printMessage(s"Number of commits fixing bugs in $Language repositories: $NumberOfCommits")
      commitsFixDf.show(RowsToShow)
    }


    // 7.
    def committersPerProject(engine: Engine): Unit = {
      val committersDf = commitsDf
        .getAllReferenceCommits
        .groupBy('repository_id)
        .agg(size(collect_set('author_name)) as "committers")
        .orderBy(desc("committers"))
        .cache()

      printMessage("Number of committers per project:")
      committersDf.show(RowsToShow, false)
    }

    // 9. Number of commits in master branch per year for projects which contains some 'Lang' files.
    def numberOfCommitsXLangProjectPerYear(engine: Engine): Unit = {
      val Language = "Java"
      val langsDf = commitsDf
        .getBlobs
        .classifyLanguages

      val repos: Seq[String] = getReposLang(Language, langsDf)
      val reposB: Broadcast[Seq[String]] = engine.session.sparkContext.broadcast(repos)

      val yearDf = commitsDf.withColumn("year", year('committer_date))

      val commitsPerYearDf = yearDf
        .groupBy('repository_id)
        .pivot("year")
        .agg(size(collect_set("hash")))
        .cache()

      val reposDf = commitsPerYearDf.filter('repository_id.isin(reposB.value: _*))

      val nullCols: Array[String] = reposDf.columns.filter(column =>
        reposDf.filter(reposDf(column).isNull).count() == reposDf.count()
      )

      printMessage(s"Number of commits per year for projects using $Language language:")
      reposDf.drop(nullCols: _*).show(RowsToShow, false)
    }

    private def getReposLang(lang: String,
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

    // 10. How often are popular Java build systems used?
    def javaBuildSystems(engine: Engine): Unit = {
      val Language = "Java"

      val filteredBlobsDf = blobsDf
        .filter('lang.isin("Maven POM", "Ant Build System", "Gradle")
          || 'path.contains("build.sbt"))

      val buildSystemsDf = filteredBlobsDf
        .map(row => {
          val lang = row.getAs[String]("lang")
          val buildSystem = lang match {
            case "Maven POM" => "Apache Maven"
            case "Ant Build System" => "Apache Ant"
            case "Gradle" => "Gradle"
            case "Scala" => "sbt"
          }

          (row.getAs[String]("repository_id"), buildSystem)
        }).toDF("repository_id", "build_system")
        .dropDuplicates()
        .groupBy('build_system)
        .agg(count('repository_id) as "repositories")
        .cache()

      printMessage(s"Number of $Language repositories per Build System:")
      buildSystemsDf.show(false)
    }

  }

  /*
  LEGAL
  1. What are the five most used licenses?
  2. How many projects use more than one license?
  */

  private object Legal extends {

    val queries: Seq[(Engine) => Unit] = Seq(
      mostUsedLicenses,
      projectsUsingMoreThanOneLicense
    )

    //1.
    def mostUsedLicenses(engine: Engine): Unit = {
      val licenseDf = blobsDf
        .filter('path.contains("LICENSE") || 'path.contains("license") || 'path.contains("License"))
        .withColumn("license", identifyLicense('content))
        .groupBy('license)
        .count()
        .orderBy('count)
        .cache()

      printMessage("Most used licenses:")
      licenseDf.show(RowsToShow, false)
    }

    //2.
    def projectsUsingMoreThanOneLicense(engine: Engine): Unit = {
      val licensesDf = blobsDf
        .filter('path.contains("LICENSE") || 'path.contains("license") || 'path.contains("License"))
        .withColumn("license", identifyLicense('content))
        .groupBy('repository_id)
        .agg(collect_set('license) as "licenses")
        .filter(size('licenses) > 1)
        .cache()

      val NumberOfProjects = licensesDf.count()
      printMessage(s"Projects using more than one license: $NumberOfProjects")
      licensesDf.show(false)
    }

  }

  /*
  PLATFORM/ENVIRONMENT
  !!! 1. What are the five most supported operating systems?
  !!! 2. Which projects support multiple operating systems?
  3. What are the five most popular databases?
  4. What are the projects that support multiple databases?
  5. How often is each database used in each programming language?
  */

  private object PlatformEnvironment {

    val queries: Seq[(Engine) => Unit] = Seq(
      mostPopularDatabases,
      dbUsagePerBlobLang
    )

    // 3. and 4.
    def mostPopularDatabases(engine: Engine): Unit = {
      val databasesDf = blobsDf
        .withColumn("databases", lookForDbs('content))
        .filter(size('databases) > 0)
        .select('repository_id, 'blob_id, 'path, 'databases)

      val reposDatabaseDf = databasesDf
        .groupBy('repository_id)
        .agg(collect_set('databases) as "db")
        .map(row => {
          val databases = row.getAs[mutable.WrappedArray[mutable.WrappedArray[String]]]("db")
            .flatten.distinct.toArray

          (row.getString(0), databases)
        }).toDF("repository_id", "databases")
        .cache()

      printMessage(s"Datbases used per repository:")
      reposDatabaseDf.show(RowsToShow, false)

      val reposPerDbDf = reposDatabaseDf
        .withColumn("database", explode('databases))
        .groupBy('database)
        .count()
        .withColumnRenamed("count", "repositories_amount")
        .orderBy('repositories_amount.desc)
        .cache()

      printMessage(s"Number of repositories using a database:")
      reposPerDbDf.show(RowsToShow, false)
    }

    def dbUsagePerBlobLang(engine: Engine): Unit = {
      val databasesDf = blobsDf
        .withColumn("databases", lookForDbs('content))
        .filter(size('databases) > 0)
        .select('repository_id, 'blob_id, 'path, 'lang, 'databases)

      val dbUsagePerLangDf = databasesDf
        .groupBy('lang)
        .agg(collect_list('databases) as "databases")
        .map(row => {
          val databases = row.getAs[mutable.WrappedArray[mutable.WrappedArray[String]]]("databases")
            .flatten.toArray

          (row.getString(0), databases)
        }).toDF("lang", "databases")
        .withColumn("database", explode('databases))
        .groupBy('lang)
        .pivot("database")
        .count()

      printMessage(s"Databases per blob per lang:")
      dbUsagePerLangDf.show(RowsToShow, false)
    }

  }

  /*
  SOURCE CODE
  1. What are the five largest projects, in terms of AST nodes?
  2. How many valid Java files in latest snapshot? (invalid Python files in latest snapshot)

  !!! 3. How many fixing revisions added null checks?
  !!! 4. What files have unreachable statements?

  5. How many generic fields are declared in each project?
  6. How is varargs used over time?
  7. How is transient keyword used in Java?(public instead of transient, How -> How many times is used per project)
  */

  private object SourceCode {

    val queries: Seq[(Engine) => Unit] = Seq(
      largestProjectsPerASTNodes,
      invalidLangFilesInLatestSnapshot,
      fieldsPerJavaProject,
      varargsPerJavaRepoPerYear,
      keywordUsagePerLangRepository
    )

    // 1.
    def largestProjectsPerASTNodes(engine: Engine): Unit = {
      val reposDf = blobsDf
        .where("lang='Python' OR lang='Java'")
        .extractUASTs()
        // .filter(size('uast) > 0)
        .queryUAST("//*", "uast", "result")
        .withColumn("uast_nodes", size('result))
        .groupBy('repository_id)
        .sum("uast_nodes")
        .withColumnRenamed("sum(uast_nodes)", "uast_nodes")
        .orderBy('uast_nodes.desc)
        .cache()

      val NumOfRepos = 5
      printMessage(s"$NumOfRepos largest projects in terms of AST nodes:")
      reposDf.show(NumOfRepos, false)
    }

    // 2.
    def invalidLangFilesInLatestSnapshot(engine: Engine): Unit = {
      val Language = "Python"

      val invalidBlobsDf = blobsDf
        .where(s"lang='$Language'")
        .extractUASTs()
        // .filter(size('uast) === 0)
        .select('repository_id, 'blob_id, 'path, 'uast)

      val invalidBlobsPerRepoDf = invalidBlobsDf
        .groupBy('repository_id)
        .agg(collect_set('blob_id) as "invalid_blobs")
        .withColumn("invalid_blobs_amount", size('invalid_blobs))
        .cache()

      printMessage(s"Invalid $Language blobs in the latest commits:")
      invalidBlobsPerRepoDf.show(RowsToShow)
    }


    // 5.
    def fieldsPerJavaProject(engine: Engine): Unit = {
      val Language = "Java"
      val Query = "//FieldDeclaration"

      val fieldsDf = blobsDf
        .where(s"lang='$Language'")
        .extractUASTs()
        .queryUAST(Query)
        .withColumn("fields", size('result))
        .groupBy('repository_id)
        .agg(sum('fields) as "fields")
        .cache()

      printMessage(s"Number of generic fields per $Language repository:")
      fieldsDf.show(RowsToShow, false)
    }

    // 6.
    def varargsPerJavaRepoPerYear(engine: Engine): Unit = {
      val Language = "Java"
      val Query = "//MethodDeclaration/SingleVariableDeclaration[@varargs='true']"

      val lastCommitPerYearDf = commitsDf
        .getAllReferenceCommits
        .select('repository_id, 'hash, 'committer_date)
        .withColumn("year", year('committer_date))
        .groupBy('repository_id, 'year)
        .agg(max('committer_date) as "committer_date")
        .join(commitsDf.getAllReferenceCommits, Seq("repository_id", "committer_date"))
        .withColumnRenamed("repository_id", "repo_id")

      val varargsPerCommitDf = blobsDf
        .where(s"lang='$Language'")
        .extractUASTs()
        .queryUAST(Query)
        // .filter(size('result) > 0)
        .groupBy('repository_id, 'commit_hash)
        .agg(sum(size('result)) as "varargs_amount")
        .join(lastCommitPerYearDf)
        .where('commit_hash === 'hash and 'repository_id === 'repo_id)

      val varargsPerRepoPerYearDf = varargsPerCommitDf
        .groupBy('repository_id)
        .pivot("year")
        .sum("varargs_amount")

      printMessage(s"Varargs used in the last commit per year for each $Language repository:")
      varargsPerRepoPerYearDf.show
    }

    // 7. How many times certain keyword is used per lang project
    def keywordUsagePerLangRepository(engine: Engine): Unit = {
      val Language = "Java"
      val Keyword = "public"

      val keywordUsageDf = blobsDf
        .where(s"lang='$Language'")
        .extractUASTs()
        .queryUAST(s"//*[@token='$Keyword']")
        .withColumn("keyword_amount", size('result))
        .groupBy('repository_id)
        .sum("keyword_amount")
        .withColumnRenamed("sum(keyword_amount)", "keyword_amount")
        .cache()

      printMessage(s"Usage of keyword '$Keyword' per $Language repository:")
      keywordUsageDf.show(RowsToShow, false)
    }

  }

  /*
  SOFTWARE ENGINEERING METRICS
  1. What are the number of attributes (NOA), per-project and per-type?
  2. What are the number of public methods (NPM), per-project and per-type?
  */

  private object SoftwareEngineeringMetrics extends {

    val queries: Seq[(Engine) => Unit] = Seq(
      attributesPerJavaProjectPerType,
      publicMethodsPerJavaProject
    )

    // 1.
    def attributesPerJavaProjectPerType(engine: Engine): Unit = {
      val Language = "Java"
      val Query = "//FieldDeclaration//PrimitiveType[@token] |" +
        "//FieldDeclaration//SimpleType/SimpleName[@token]"

      val attributesPerBlobDf = blobsDf
        .where(s"lang='$Language'")
        .extractUASTs()
        .queryUAST(Query)
        .extractTokens()
      // .filter(size('result) > 0)


      val attributesRepoDf = attributesPerBlobDf
        .groupBy('repository_id)
        .agg(collect_list('tokens) as "tokens")
        .map(row => {
          val tokens = row.getAs[mutable.WrappedArray[mutable.WrappedArray[String]]]("tokens")
            .flatten.toArray

          (row.getString(0), tokens)
        }).toDF("repository_id", "tokens")
        .withColumn("token", explode('tokens))
        .select('repository_id, 'token)
        .cache()

      val attributesPerRepoDf = attributesRepoDf
        .groupBy('repository_id)
        .agg(collect_set('token) as "tokens")
        .cache()

      printMessage(s"List of attributes types used per $Language repository:")
      attributesPerRepoDf.show(RowsToShow)

      val attributesPerRepoPerTypeDf = attributesRepoDf
        .groupBy('repository_id)
        .pivot("token")
        .count()
        .cache()

      printMessage(s"Number of attributes used per type and per $Language repository:")
      attributesPerRepoPerTypeDf.show(RowsToShow, false)
    }

    // 2. Number of public methods per Java project
    def publicMethodsPerJavaProject(engine: Engine): Unit = {
      val Language = "Java"
      val Query = "//MethodDeclaration[Modifier[@token='public']]"

      val blobsDf = engine
        .getRepositories
        .filter(size('urls) > 0)
        .getMaster
        .getCommits
        .getBlobs
        .classifyLanguages

      val publicMethodsPerRepoDf = blobsDf
        .where(s"lang='$Language'")
        .extractUASTs()
        .queryUAST(Query)
        .filter(size('result) > 0)
        .groupBy('repository_id)
        .agg(sum(size('result)) as "public_methods")
        .cache()

      printMessage("Public Methods per Java project:")
      publicMethodsPerRepoDf.show(RowsToShow, false)
    }

  }

}
