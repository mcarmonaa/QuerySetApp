SHELL = /bin/sh

# Scala version
SCALA_VERSION ?= 2.11.12

# Spark home
SPARK_HOME ?= /usr/local/spark

# SBT
SBT = ./sbt ++$(SCALA_VERSION)

# Submit spark app args
APP_NAME = "QuerySetApp"
APP_MAIN_CLASS = "tech.sourced.queryset.Main"
SPARK_MASTER ?= "local[*]"
UBER_JAR = "target/scala-2.11/queryset-0.1.0.jar"
REPOS_PATH ?= "src/main/resources/siva-files"
REPOS_FORMAT ?= "siva"
APP_ARGS = $(REPOS_PATH) $(REPOS_FORMAT) 

# Rules
all: clean build

.PHONY: run build clean
run:
	$(SPARK_HOME)/bin/spark-submit \
	--name $(APP_NAME) \
	--class $(APP_MAIN_CLASS) \
	--master $(SPARK_MASTER) \
	$(UBER_JAR) $(APP_ARGS)

build: clean
	$(SBT) assembly

clean:
	$(SBT) clean
