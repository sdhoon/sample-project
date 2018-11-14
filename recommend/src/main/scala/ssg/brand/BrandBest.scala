package ssg.brand

import com.datastax.driver.core.utils.UUIDs
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import ssg.util.{BrandBestData, Reader}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

/**
  * Created by moneymall on 13/07/16.
  */
object BrandBest {

  def main(args:Array[String]) {
    val executor = new Executor
    executor.init()
    executor.run()
  }

}

class Executor {

  private var hiveContext : HiveContext = _
  private var sc : SparkContext = _

  def init() {
    val sparkConf = new SparkConf()
      .setAppName("BrandBest")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.local.dir","/data02/spark_tmp/")
      .set("spark.buffer.pageSize", "16m")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "256m")
      .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
      .set("spark.executor.uri","hdfs://master001p27.prod.moneymall.ssgbi.com:9000/user/spark/spark-1.6.1-bin-hadoop2.6.tgz")
      .set("spark.cassandra.connection.host", "10.203.9.228")
      .set("spark.cassandra.connection.host", "10.203.9.224")

    sc = new SparkContext(sparkConf)
    hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.setConf("spark.sql.tungsten.enabled", "false")

    //truncate recommend.brandbest
    CassandraConnector(sparkConf).withSessionDo(session =>
    session.execute("TRUNCATE recommend.brandbest"))

  }

  def run() {

    val reader = new Reader(sc, hiveContext)

    val brand = reader.getBrand()
    brand.registerTempTable("BRAND")

    val item = reader.item()
    item.registerTempTable("ITEM")

    val search = reader.getSearchBestEachType()
    search.registerTempTable("SEARCH")

    val brandBest = hiveContext.sql(SQLMaker.getBrandRank)

    brandBest.map(t => "%s,%s,%s,%s".format(
      t(0), //brandId
      t(1), //brandNm
      t(2), //itemId
      t(3) //score
    )).saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/brand/output/" + reader.getDate() + "/")

    val cassandraData = brandBest.map(t =>
      BrandBestData(
        t(0).toString,  //brandId
        t(1).toString,  //brandNm
        t(2).toString,  //itemId
        t(3).toString.toDouble,  //score
        UUIDs.timeBased()
      ))

    cassandraData.saveToCassandra("recommend", "brandbest", SomeColumns("brand_id", "brand_nm", "item_id", "score", "reg_dts"))




  }
}
