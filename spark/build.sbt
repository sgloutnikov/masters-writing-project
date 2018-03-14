name := "masters-writing-project"

version := "0.1"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "Spark Packages" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.1",
  "databricks" % "spark-corenlp" % "0.2.0-s_2.11",
  "com.google.protobuf" % "protobuf-java" % "3.5.1",
  "com.databricks" %% "spark-csv" % "1.5.0"
)