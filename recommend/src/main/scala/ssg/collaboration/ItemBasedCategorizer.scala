package ssg.collaboration

/*
    Author  : YoungGyu.C
    Date    : 2016-04-23
    Info    :
*/


import java.io.Serializable
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, UUID}
import com.datastax.driver.core.utils.UUIDs


import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import ssg.util.{CassandraItemBased, Reader}





class ExecutorCategorizer extends Serializable{
  private var hiveContext : HiveContext = _
  private var sc : SparkContext = _


  def init() {
   /* val sparkConf = new SparkConf()
      .setAppName("ssg.collaboration.ItemBasedCategorizer")
      .set("spark.local.dir","/data01/spark_tmp/")
      .set("spark.executor.uri","hdfs://master001p27.prod.moneymall.ssgbi.com:9000/user/spark/spark-1.6.2-bin-hadoop2.6.tgz")
      .set("spark.cassandra.connection.host", "10.203.9.224")
      .set("spark.cassandra.connection.host", "10.203.9.228")*/
//      .set("spark.cassandra.connection.host", "10.203.3.51")
//      .set("spark.cassandra.connection.host", "10.203.3.52")
//      .set("spark.cassandra.connection.host", "10.203.3.53")
      /*.set("spark.cassandra.output.consistency.level", "LOCAL_ONE")
      .set("spark.cassandra.output.concurrent.writes", "1")
      .set("spark.cassandra.output.batch.size.bytes", "512")
      .set("spark.cassandra.output.throughput_mb_per_sec", "0.5")
*/
    sc = new SparkContext()
    hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //hiveContext.setConf("spark.sql.tungsten.enabled", "false")
  }


  def run(types:String, date:String, isFirstLoaded:Boolean) {

    val dt = date.split("-")
    val calendar = Calendar.getInstance()
    val sd = new SimpleDateFormat("yy-MM-dd")
    val tempDate = new Date(100+dt(0).toInt,dt(1).toInt-1,dt(2).toInt,0,0,0)
    calendar.setTime(tempDate)
    calendar.add(Calendar.DATE, -1)
    val yesterday = sd.format(calendar.getTime)

    val reader = new Reader(sc, hiveContext)

    val preItem = reader.item()
    preItem.registerTempTable("ITEM")

    val stdCtg = reader.stdCtgUTF8
    stdCtg.registerTempTable("STD_CTG")
    /*stdCtg.show(10)*/

    val itemBased = reader.itemBased(types, date)
    itemBased.registerTempTable("ITEM_BASED")

    val item = hiveContext.sql(SQLMaker.getItem)
    item.registerTempTable("TARGET_ITEM")
    //item.show(100)


    val getItem2 = hiveContext.sql(SQLMaker.getItem2)
    getItem2.registerTempTable("RECOM_ITEM")

    val categorized = hiveContext.sql(SQLMaker.getCategory())
    categorized.registerTempTable("CATEGORIZED_ITEM")

    val site = reader.lwstPrice
    site.registerTempTable("SITE")

    val result = hiveContext.sql(SQLMaker.getSiteNo())





    result.map(t => "%s,%s,%s,%s,%s,%s".format(
        t(0)  //item_id
      , t(1)  //recommended_item_id
      , t(2)  //siteNo
      , t(3)  //score
      , t(4).toString.concat("^").concat(t(5).toString).concat("^").concat(t(6).toString).concat("^").concat(t(7).toString)
      , t(8).toString.concat("^").concat(t(9).toString).concat("^").concat(t(10).toString).concat("^").concat(t(11).toString)))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/cf/itembased/output/categorize/" + types + "/" + date + "/")


    val itemBasedToday = reader.getCategorize(types, date)
    itemBasedToday.registerTempTable("ITEMBASE_TODAY")

    println("############### ItemBasedCategorizer DATE : " +  date + " ItemBasedCategorizer END ###############")

    if(!isFirstLoaded){
      println("###########NO FIRST LOAD############")
      val itemBasedYesterday = reader.getCategorize(types, yesterday)
      itemBasedYesterday.registerTempTable("ITEMBASE_YESTERDAY")
    }else{
      println("###########FIRST LOAD############")
      val itemBasedYesterday = itemBasedToday
      itemBasedYesterday.registerTempTable("ITEMBASE_YESTERDAY")
    }


    val categorizedData = hiveContext.sql(SQLMaker.getAvgScore())

    categorizedData.map(t => "%s,%s,%s,%s,%s,%s,%s,%s,%s".format(
      t(0), //itemId
      t(1), //stdCtg
      t(2), //recommendItemId
      t(3), //recommendStdCtg
      t(4), //method
      t(5), //yesterdayScore
      t(6), //todayScore
      t(7), //score
      t(8) //siteNo
    )).saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/cf/itembased/output/cassandra/" + types + "/" + date + "/")

    val cassandraData = categorizedData.map(t =>
      CassandraItemBased(
        t(0).toString,  //itemId
        t(2).toString,  //recommendItemId
        t(7).toString,  //score
        t(4).toString,  //method
        t(8).toString, //siteNo
        UUIDs.timeBased())) //regDts

    cassandraData.saveToCassandra("recommend", "itembase", SomeColumns("item_id", "recommend_item_id", "score", "method", "site_no", "reg_dts"))
  }


}

object ItemBasedCategorizer{
  def main(args:Array[String]) {
    val executor = new ExecutorCategorizer
    executor.init()

    //args(0) = method
    //args(1) = date
    //args(02 = FIRSTLOAD

    var isFirstLoaded = false;
    if(args.size == 3 && args(2).equals("FIRSTLOAD")){
      isFirstLoaded = true;
    }


    executor.run(args(0), args(1), isFirstLoaded)
  }
}