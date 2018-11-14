package ssg.category

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.SomeColumns
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import ssg.util.{CategoryBestData, Reader}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

/**
  * Created by moneymall on 25/07/16.
  */
object CategoryBest {

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
      session.execute("TRUNCATE recommend.categorybest"))

  }

  def run() {

    val reader = new Reader(sc, hiveContext)

    val category = reader.stdCtgUTF8
    category.registerTempTable("CATEGORY")

    val item = reader.item()
    item.registerTempTable("ITEM")

    val search = reader.getSearchBestEachType()
    search.registerTempTable("SEARCH")

    val categoryBest = hiveContext.sql(SQLMaker.getCategoryBest)

    categoryBest.map(t => "%s,%s,%s,%s".format(
      t(0), //stdCtgDclsId
      t(1), //stdCtgDclsNm
      t(2), //itemId
      t(3) //score
    )).saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/category/output/" + reader.getDate() + "/")

    val cassandraData = categoryBest.map(t =>
      CategoryBestData(
        t(0).toString,  //stdCtgDclsId
        t(1).toString,  //stdCtgDclsNm
        t(2).toString,  //itemId
        t(3).toString.toDouble,  //score
        UUIDs.timeBased()
      ))

    cassandraData.saveToCassandra("recommend", "categorybest", SomeColumns("std_ctg_id", "std_ctg_nm", "item_id", "score", "reg_dts"))
  }
}

