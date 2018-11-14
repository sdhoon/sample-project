package ssg.item

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.SomeColumns
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import ssg.util.{ItemMasters, TableDivision, Reader}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

/**
  * Created by moneymall on 26/08/16.
  */
object ItemMaster {

  def main(args:Array[String]) {
    val executor = new Executor
    executor.init()
    executor.run()
  }

}

class Executor {

  private var hiveContext : HiveContext = _
  private var sc : SparkContext = _
  private var code="";

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
      .set("spark.cassandra.connection.host", "10.203.3.52")
      .set("spark.cassandra.connection.host", "10.203.3.53")
      .set("spark.cassandra.connection.host", "10.203.3.54")

    sc = new SparkContext(sparkConf)
    hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.setConf("spark.sql.tungsten.enabled", "false")

    val rdd = sc.cassandraTable("recommend", "table_division").select("code").where("div = ?", "ITEM_DIV_CD")
    val row = rdd.first()
    val codeResult = row.getString("code");


    if("10".equals(codeResult)){
      code = "20";
    }else if("20".equals(codeResult)){
      code = "10";
    }

    println("CODERESULT : + " + codeResult);

    println("CODE : + " + code);

    //truncate recommend.item_10 or item_20
    CassandraConnector(sparkConf).withSessionDo(session =>
      session.execute("TRUNCATE recommend.item_"+code))
      //session.execute("DELETE FROM recommend.item_"+code))

  }

  def run() {

    val reader = new Reader(sc, hiveContext)
    val item = reader.item()
    item.registerTempTable("ITEM")

    val itemMaster = hiveContext.sql(SQLMaker.getItem)
    //itemMaster.show()

    itemMaster.map(t => "%s,%s,%s".format(
      t(0), //itemId
      t(1), //brandId
      t(2) //stdCtgId
    )).saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/item/output/" + reader.getDate() + "/")

    val cassandraData = itemMaster.map(t =>
      ItemMasters(
        t(0).toString, //itemId
        t(1).toString, //brandId
        t(2).toString //stdCtgId
      ))

    cassandraData.saveToCassandra("recommend", "item_"+code, SomeColumns("item_id", "brand_id", "std_ctg_id"))

    val codeData = sc.makeRDD(Seq(TableDivision("ITEM_DIV_CD", code)))
    codeData.saveToCassandra("recommend", "table_division", SomeColumns("div", "code"))
  }
}
