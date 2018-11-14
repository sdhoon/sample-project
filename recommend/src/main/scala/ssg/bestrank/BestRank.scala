package ssg.bestrank

/*
    Author  : YoungGyu.C
    Date    : 2015-09-21
    Info    :
*/

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import ssg.util.{Reader}


class Executor {

  private val TRACK_ITEM_DTL_DAY = 7
  private var hiveContext : HiveContext = _
  private var sc : SparkContext = _

  def init() {
    val sparkConf = new SparkConf()
      .setAppName("Recommend_BestRank")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.local.dir","/data02/spark_tmp/")
      .set("spark.buffer.pageSize", "16m")
      .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
      .set("spark.executor.uri","hdfs://master001p27.prod.moneymall.ssgbi.com:9000/user/spark/spark-1.6.2-bin-hadoop2.6.tgz")

    sc = new SparkContext(sparkConf)
    hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.setConf("spark.sql.tungsten.enabled", "false")

  }

  def run(){

    val reader = new Reader(sc, hiveContext)

    val preItem = reader.item()
    preItem.registerTempTable("PRE_ITEM")

    val itemPrice = reader.lwstPrice
    itemPrice.registerTempTable("ITEM_PRICE")

    val preOrdItem = reader.ordItem
    preOrdItem.registerTempTable("PRE_ORD_ITEM")

    val preTrackItem = reader.trackItemDtl(TRACK_ITEM_DTL_DAY)
    preTrackItem.registerTempTable("PRE_TRACK")

    val eventDF = reader.event
    eventDF.registerTempTable("EVENT")

    val item = hiveContext.sql(SQLMaker.getItem)
    item.registerTempTable("ITEM")

    val trackItemDtl = hiveContext.sql(SQLMaker.getTrack)
    trackItemDtl.registerTempTable("TRACK_ITEM_DTL")

    val ordItemCnt = hiveContext.sql(SQLMaker.getOrdItemCnt())
    ordItemCnt.registerTempTable("ORD_ITEM_CNT")

    val trkItemCnt = hiveContext.sql(SQLMaker.getTrkItemCnt)
    trkItemCnt.registerTempTable("TRACK_ITEM_CNT")

    val eventCnt = hiveContext.sql(SQLMaker.getEventCnt)
    eventCnt.registerTempTable("EVENT_CNT")

    val union = hiveContext.sql(SQLMaker.ordTrkUnion)
    union.registerTempTable("ORD_TRK_UNION")


    val itemOrdTrack = hiveContext.sql(SQLMaker.itemOrdTrack)
    itemOrdTrack.registerTempTable("ITEM_ORD_TRK")

    val ordCntRank = hiveContext.sql(SQLMaker.ordCntRank)
    ordCntRank.registerTempTable("ORD_CNT_RANK")

    val excluding6005 = hiveContext.sql(SQLMaker.excluding6005)
    excluding6005.registerTempTable("EXCLUDE_6005")

    val including6005 = hiveContext.sql(SQLMaker.including6005)
    including6005.registerTempTable("INCLUDE_6005")

    val merge = hiveContext.sql(SQLMaker.merge)
    val unionData = merge.unionAll(including6005)
    unionData.registerTempTable("UNION_DATA")

    val finalRank = hiveContext.sql(SQLMaker.finalRank)
    finalRank.registerTempTable("FINAL_RANK")

    finalRank
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //dispSiteNo
          ,a(2) //sellSiteNo
          ,a(3) //ordCnt
          ,a(4) //ordCntRank
          ,a(5) //brandId
          ,a(6) //nitmYn
          ,a(7) //prodManufCntryId
          ,a(8) //trkItemCnt
          ,a(9) //trkItemCntRank
          ,a(10)//sellPrc
          ,a(11)//sellPrcRank
          ,a(12)//totalScore
          ,a(13)//totalRank
          ,a(14)//event
          ,a(15)//finalRank
          ,a(16)))//createDate
      .coalesce(1)
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/merge_ALL__" + reader.getDate)

    finalRank
      .map(a => "%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //dispSiteNo
          ,a(2) //sellSiteNo
          ,a(15)//finalRank
          ,a(16)))//createDate
      .coalesce(1)
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/campaign_" + reader.getDate)

    val stdCtg = reader.stdCtg
    stdCtg.registerTempTable("STD_CTG")

    val prodData = hiveContext.sql(SQLMaker.prodData)
    prodData
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //CRITN_DT
          ,a(1) //DISP_SITE_NO
          ,a(2) //SELL_SITE_NO
          ,a(3) //ITEM_ID
          ,a(4) //BRAND_ID
          ,a(5) //ORD_CONT
          ,a(6) //ORD_AMT
          ,a(7) //RECOM_REG_CNT
          ,a(8) //eval_scr
          ,a(9)//LIVSTK_KIND_CD
          ,a(10)//LIVSTK_PAT_CD
          ,a(11)//ORPLC_ID
          ,a(12)//NITM_YN
          ,a(13)//REGPE_ID
          ,a(14)//REG_DTS
          ,a(15)//MODPE_ID
          ,a(16)))//MOD_DTS
      .coalesce(1)
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/prod_" + reader.getDate)

  }


}
object BestRank{
  def main(args:Array[String]) {
    val executor = new Executor
    executor.init()
    executor.run()
  }
}