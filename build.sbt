name := "SparkSeaweedExample"
version := "0.1"
scalaVersion := "2.12.10"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
  "org.apache.hadoop" % "hadoop-aws" % "3.2.0",
  "org.apache.hadoop" % "hadoop-common" % "3.2.0",
  "org.apache.hadoop" % "hadoop-client" % "3.2.0",
  "com.github.chrislusf" % "seaweedfs-client" % "1.6.9"
  /* "com.github.chrislusf" % "seaweedfs-hadoop2-client" % "1.6.9" */
)
