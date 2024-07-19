
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark_neo4j_kafka"
  )
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11" % Test
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.5.1" % "provided"
libraryDependencies += "org.neo4j" %% "neo4j-connector-apache-spark" % "5.3.1_for_spark_3"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1" % "provided"


