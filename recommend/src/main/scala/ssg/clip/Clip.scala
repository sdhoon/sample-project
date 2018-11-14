package ssg.clip

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import ssg.util.{ClipItem, Reader}
import com.datastax.spark.connector._

/**
  * Created by 131326 on 2016-11-30.
  */
object Clip {

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
    .setAppName("Clip")
    .set("spark.local.dir","/data02/spark_tmp/")
    .set("spark.executor.uri","hdfs://master001p27.prod.moneymall.ssgbi.com:9000/user/spark/spark-1.6.2-bin-hadoop2.6.tgz")
    //.set("spark.cassandra.connection.host", "10.203.9.224")
    //.set("spark.cassandra.connection.host", "10.203.9.228")

    sc = new SparkContext()
    hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sc.cassandraTable("clip", "clip").deleteFromCassandra("clip", "clip")

  }

  def run() {
    val reader = new Reader(sc, hiveContext)
    val item = reader.item()
    item.registerTempTable("ITEM")

    val searchBest = reader.getSearchBestEachType()
    searchBest.registerTempTable("SEARCH")

    //브랜드별 신상품
    val newItem = hiveContext.sql(SQLMaker.getNewItem)
    val newItemCassandra = newItem.map( row =>
      ClipItem(
        row(0).toString, //brandId
        row(1).toString, //kind
        row(2).toString, //itemId
        row(3).toString //rank
      )
    )
    newItemCassandra.saveToCassandra("clip", "clip", SomeColumns("brand_id", "kind", "id", "rank"))


    //브랜드별 베스트
    val best = hiveContext.sql(SQLMaker.getBestItem())
    val bestCassandra = best.map ( row =>
      ClipItem(
        row(0).toString, //brandId
        row(1).toString, //kind
        row(2).toString, //itemId
        row(3).toString //rank
      )
    )
    bestCassandra.saveToCassandra("clip", "clip", SomeColumns("brand_id", "kind", "id", "rank"))

    //read itembase from cassandra
//    val cassandraItemBase = sc.cassandraTable("recommend","itembase")
//                 .select("item_id", "site_no", "recommend_item_id", "reg_dts", "score")
//
//    cassandraItemBase.map( row =>
//        row.getString("item_id") + "," +
//        row.getString("site_no") + "," +
//        row.getString("recommend_item_id")+ "," +
//        row.getString("reg_dts")+ "," +
//        row.getString("score")
//    ).saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/cassandra/itembase" + reader.getDate())



    //연관 브랜드
    /*val itemBase = reader.getItemBase()
    itemBase.registerTempTable("ITEMBASE")

    val associationBrand = hiveContext.sql(SQLMaker.getAssociationBrand())
    val cassandraAssociationBrand = associationBrand.map(row =>
      ClipItem(
        row(0).toString, //brandId
        row(1).toString, //kind
        row(2).toString, //associationBrandId
        row(3).toString)) //rank
    cassandraAssociationBrand.saveToCassandra("clip", "clip", SomeColumns("brand_id", "kind", "id", "rank"))*/
  }
}
