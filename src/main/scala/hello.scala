import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions}

import java.net.{HttpURLConnection, URL}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, Future}

case class Chunk(index: Int, fid: String, vid: Int, path: String)

class VolumePartition(volumeId: Int) extends Partition {
  override def index: Int = volumeId
}


class SeaweedRDD(
                sc: SparkContext,
                paths: Set[String],
                chunksInfo: Map[Int, Chunk],
                volumesInfo: Map[Int, String],
                ) extends RDD[(String, Array[Byte])](sc, Seq()) {

  private val m = volumesInfo.keys.toArray

  override def compute(split: Partition, context: TaskContext): Iterator[(String, Array[Byte])] = {
    val i = volumesInfo.keys.toArray.apply(split.index)
    val url = volumesInfo.get(i).get
    chunksInfo
      .filter(kv => kv._1 == m(split.index) && paths.contains(kv._2.path))
      .values
      .toSeq
      .sortBy(ch => {ch.path})
      .map(chunk => (chunk.path, Await.result(loadChunk(url, chunk.fid), Duration.create(5, SECONDS))))
      .iterator
  }

  def loadChunk(url: String, fid: String): Future[Array[Byte]] = Future {
    val connection = new URL(s"http://$url/$fid").openConnection().asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(5000)
    connection.setReadTimeout(5000)
    connection.connect()
    connection.getInputStream.readAllBytes()
  }

  override protected def getPartitions: Array[Partition] = {
    volumesInfo.keys.toArray.indices.map(i => new Partition { val index: Int = i}).toArray
  }

}


object Hello {
  def main(args: Array[String]): Unit = {
    println(args)
    val sparkConf = new SparkConf()
     .setAppName("SparkSeaweedExample")
     .setMaster("local[1]")
     // .setMaster("spark://spark:7077")
      // .setJars(Seq("/Users/m/code/spark-seaweedfs/lib/seaweedfs-hadoop2-client-1.6.9.jar"))
     .set("spark.hadoop.fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem")
     .set("spark.hadoop.fs.defaultFS", "seaweedfs://seaweedfs:8888")
     // .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
     // .set("spark.hadoop.fs.s3a.endpoint", "org.apache.hadoop.fs.s3a.S3AFileSystem")
     // .set("spark.hadoop.fs.s3a.access.key", "")
     // .set("spark.hadoop.fs.s3a.secret.key", "")
    // .set("spark.dynamicAllocation.enabled", "false")
    // val sc = new SparkContext(sparkConf)
    // val hadoopConfig = sc.hadoopConfiguration
    // hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    // val hadoopConfig = spark.SparkContext.hadoopConfiguration
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val rdd = new SeaweedRDD(
      sc = spark.sparkContext,
      paths = Set("/buckets/test-bucket/vk/_3528/_1747/4"),
      chunksInfo = Map((4, Chunk(index = 0, fid = "4,214fcca9438d", vid = 4, path = "/buckets/test-bucket/vk/_3528/_1747/4"))),
      volumesInfo = Map((4, "192.168.105.100:8082"))
    )
    rdd.collect().foreach(kv => println(s"key: ${kv._1}, value: ${kv._2.map("%02X" format _).mkString}"))
    // val df = spark
    //   .read
    //   .format("binaryFile")
    //   .option("pathGlobFilter", "*")
    //   .load("seaweedfs://seaweedfs:8888/test-bucket/vk/_1824/_6111/")
      // .load("/Users/m/code/seaweedfs/")
    // df.explain()
    // df.select(functions.sum(functions.length(col("content")))).foreach(tb => println(s"total bytes $tb"))
    spark.close()
  }
}
