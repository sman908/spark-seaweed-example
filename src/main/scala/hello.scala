import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions}


object Hello {
  def main(args: Array[String]): Unit = {
    println(args)
    val sparkConf = new SparkConf()
     .setAppName("SparkSeaweedExample")
     .setMaster("local[1]")
     // .setMaster("spark://spark:7077")
      // .setJars(Seq("/Users/m/code/spark-seaweedfs/lib/seaweedfs-hadoop2-client-1.6.9.jar"))
     .set("spark.hadoop.fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem")
     // .set("spark.hadoop.fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem")
     .set("spark.hadoop.fs.defaultFS", "s3a://seaweedfs:8888")
    // .set("spark.dynamicAllocation.enabled", "false")
    // val sc = new SparkContext(sparkConf)
    // val hadoopConfig = sc.hadoopConfiguration
    // hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    // val hadoopConfig = spark.SparkContext.hadoopConfiguration
    val df = spark.read.format("binaryFile").option("pathGlobFilter", "*.txt").load("s3a://seaweedfs:8888/test-data/")
    df.select(functions.sum(functions.length(col("content")))).foreach(tb => println(s"total bytes $tb"))
  }
}
