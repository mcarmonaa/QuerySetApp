# QuerySetApp

**QuerySetApp** is a `Scala/Spark` application developed with the purpose in mind to test performance and compatibility among different [**source{d}'s engine**](https://github.com/src-d/engine) versions applying different well know queries on repositories.

#### Submit appication to a cluster

```bash
$SPARK_HOME/bin/spark-submit \\
    - -name "QuerySetApp" \
    --class "tech.sourced.queryset.Main" \\
    --master $SPARK_MASTER \\
    path/to/queryset-0.1.0.jar $REPOS_PATH $REPOS_FORMAT
```

- `SPARK_HOME` environment variable must point to the directory where `Spark` was downloaded (e.g. `/usr/local/spark`)

- `SPARK_MASTER` environment variable must point to the [master URL](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls) for the cluster (e.g. `spark://p-spark-master:7077`)

- `REPOS_PATH` must point to the directory which contains the repositories.

- `REPOS_FORMAT` must specify the repositories' format (e.g. `siva`)

#### Development
- First you should set the `Spark` directory:

    export SPARK_HOME="path/to/spark"

- Build the fatjar to submit to a spark cluster:

    make build

It leaves the fatjar under `target/scala-2.11/queryset-0.1.0.jar`

- Run application in local, by default it uses the repositories under `src/main/resources/siva-files`:

    make run

- Submit application to a cluster:

```bash
    SPARK_MASTER="spark://p-spark-master:7077" \\
    REPOS_PATH="/path/to/repos" \\
    REPOS_FORMAT="siva" \\
    make run
```
