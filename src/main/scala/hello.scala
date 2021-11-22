import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}
import seaweedfs.client.FilerProto.Entry
import seaweedfs.client.SeaweedRead.ChunkView
import seaweedfs.client.{FilerClient, FilerProto, SeaweedInputStream}

import java.net.{HttpURLConnection, URL}
import scala.collection.JavaConverters._

case class ChunkPath(fid: String, vid: Int, path: String)

class VolumePartition(volumeId: Int) extends Partition {
  override def index: Int = volumeId
}

case class Info(
    entryToVolumeId: Entry => String,
    pathToEntry: String => Entry,
    volumesLocations: scala.collection.Map[String, List[String]],
)

class SeaweedRDD(
    sc: SparkContext,
    host: String,
    port: Int,
    grpcPort: Int,
    paths: Set[String],
    info: Info,
) extends RDD[(String, Array[Byte])](sc, Seq()) {

  override def compute(
      split: Partition,
      context: TaskContext): Iterator[(String, Array[Byte])] = {
    val filerClient = new FilerClient(host, port, grpcPort)
    val vid = info.volumesLocations.keys.toArray.apply(split.index)
    paths.toSeq
      .filter(p => {
        val entry = info.pathToEntry(p)
        vid.equals(info.entryToVolumeId(entry))
      })
      .map(p => {
        val entry = info.pathToEntry(p)
        val volumeId = info.entryToVolumeId(entry)
        val locations = info.volumesLocations(volumeId)
        val fid = entry.getChunksList.get(0).getFileId
        (p, loadChunk(locations.head, fid))
      })
      .iterator
  }

  def loadChunk(volumeUrl: String, fid: String): Array[Byte] = {
    val connection = new URL(s"http://$volumeUrl/$fid")
      .openConnection()
      .asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(5000)
    connection.setReadTimeout(5000)
    connection.connect()
    val res = connection.getInputStream.readAllBytes()
    connection.disconnect()
    res
  }

  override protected def getPartitions: Array[Partition] = {
    info.volumesLocations.keys.toArray.indices
      .map(i =>
        new Partition {
          val index: Int = i
      })
      .toArray
  }
}

object Hello {

  def loadEntries(fc: FilerClient, path: String): Seq[(String, Entry)] = {
    fc.listEntries(path)
      .asScala
      .flatMap(e => {
        if (e.getIsDirectory) {
          loadEntries(fc, path + "/" + e.getName)
        } else {
          Seq((path, e))
        }
      })
  }

  def main(args: Array[String]): Unit = {
    println(args)
    val prepareStart = System.currentTimeMillis()
//    val fc = new FilerClient("localhost", 8888, 18888)
//
//    val entries = loadEntries(fc, "/buckets/test-bucket/vk").toList
//    val entryToVolumeId = entries
//      .map({ case (path, entry) => entry })
//      .filter(e => e.getChunksList.size() == 1)
//      .map(e => e -> e.getChunksList.get(0).getFid.getVolumeId.toString)
//      .toMap
//
//    val lookupRequest = FilerProto.LookupVolumeRequest.newBuilder()
//    lookupRequest.addAllVolumeIds(entryToVolumeId.values.toSet.asJavaCollection)
//    val volumesLocations = fc.getBlockingStub
//      .lookupVolume(lookupRequest.build())
//      .getLocationsMapMap
//      .asScala
//      .mapValues(v => v.getLocationsList.asScala.toList.map(l => l.getUrl))
//      .toMap
//    val pathToEntry = entries.map({ case (p, e) => p -> e }).toMap
// val info = Info(entryToVolumeId, pathToEntry, volumesLocations)
    println(
      s"prepare running time ${System.currentTimeMillis() - prepareStart}")
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

    // val rdd = new SeaweedRDD(
    //   host = "localhost",
    //   port = 8888,
    //   grpcPort = 18888,
    //   sc = spark.sparkContext,
    //   paths = pathToEntry.keys.toSet,
    //   info = info,
    // )

    // val df = spark.createDataFrame(rdd).toDF("path", "content")
    // rdd.map({ case (p, content) => content.length })})

    // .collect()
    // .foreach(
    //   kv =>
    //     println(
    //       s"key: ${kv._1}, value: ${kv._2.map("%02X" format _).mkString}"
    //   )
    // )
    val df = spark.read
      .format("binaryFile")
      .option("pathGlobFilter", "*")
      .load("seaweedfs://seaweedfs:8888/buckets/test-bucket/*/*/*/*")
    val start = System.currentTimeMillis()
    df.select(functions.col("path"), functions.col("content"))
      .sort("path")
      .show(10, truncate = true)
    spark.close()
  }
}
