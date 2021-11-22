name := "SparkSeaweedExample"
version := "0.1"
scalaVersion := "2.12.10"
enablePlugins(PackPlugin)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "io.grpc" % "grpc-all" % "1.23.0",
  /* "org.apache.hadoop" % "hadoop-core" % "1.2.1", */
  /* "org.apache.hadoop" % "hadoop-aws" % "3.2.0", */
  /* "org.apache.hadoop" % "hadoop-common" % "3.2.0", */
  /* "org.apache.hadoop" % "hadoop-client" % "3.2.0", */
  // "com.amazonaws" % "aws-java-sdk" % "1.12.115",
//  "com.github.chrislusf" % "seaweedfs-client" % "1.7.0",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.10.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0",
   "com.github.chrislusf" % "seaweedfs-hadoop3-client" % "1.7.0",
)
