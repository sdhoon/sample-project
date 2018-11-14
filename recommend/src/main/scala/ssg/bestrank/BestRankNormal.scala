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


class bestRankNormalExecutor {

  //20170512 3일치 클릭데이터
  private val TRACK_ITEM_DTL_DAY = 3
  private var hiveContext : HiveContext = _
  private var sc : SparkContext = _

  def init() {
    val sparkConf = new SparkConf()
      .setAppName("Recommend_BestRankNormal")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.local.dir","/data02/spark_tmp/")
      .set("spark.buffer.pageSize", "16m")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "256m")
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

    preOrdItem.registerTempTable("TEMP_ORD_ITEM")

    val sd = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -3)
    val d3 = sd.format(calendar.getTime)

    // 주문 3일치로 조정
    /////////////////////////////////
    println("=================================================")
    println("ordItem Day : " + d3)
    println("=================================================")
    hiveContext.sql(SQLMaker.ordItem3Days.format(d3)).registerTempTable("PRE_ORD_ITEM")
    /////////////////////////////////

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

    //////////////////////////////// 20170510 LOG SCALE로 전환 및 정규화 ////////////////////////////////////
    //val ordCntRank = hiveContext.sql(SQLMaker.ordCntRank)
    //ordCntRank.registerTempTable("ORD_CNT_RANK")

    // ITEM_ORD_TRK를 LOG SCALE로 변환
    val logScale = hiveContext.sql(SQLMaker.logScale)
    logScale.registerTempTable("LOG_SCALE")

    //정규화를 위해, 로그 값에서 각 feature별 max min값 구하기
    val normal_var = hiveContext.sql(SQLMaker.minMax.format(("LOG_SCALE"))).head()

    val maxOrdCnt       =  normal_var(0).asInstanceOf[Float]
    val minOrdCnt       =  normal_var(1).asInstanceOf[Float]

    val maxTrkitemCnt   =  normal_var(2).asInstanceOf[Float]
    val minTrkitemCnt   =  normal_var(3).asInstanceOf[Float]

    val maxRlordAmt      =  normal_var(4).asInstanceOf[Float]
    val minRlordAmt      =  normal_var(5).asInstanceOf[Float]

    val diffOrdCnt      = maxOrdCnt     - minOrdCnt
    val diffTrkitemCnt  = maxTrkitemCnt - minTrkitemCnt
    val diffRlordAmt     = maxRlordAmt    - minRlordAmt

    println("=================================================")
    println("maxOrdCnt    : " + maxOrdCnt   )
    println("minOrdCnt    : " + minOrdCnt   )
    println("=================================================")
    println("maxTrkitemCnt    : " + maxTrkitemCnt   )
    println("minTrkitemCnt    : " + minTrkitemCnt   )
    println("=================================================")
    println("maxSellPrc    : " + maxRlordAmt   )
    println("minSellPrc    : " + minRlordAmt   )
    println("=================================================")
    println("diffOrdCnt      : " + diffOrdCnt    )
    println("diffTrkitemCnt  : " + diffTrkitemCnt    )
    println("diffRlordAmt     : " + diffRlordAmt    )
    println("=================================================")

    //정규화
    val normal = hiveContext.sql(SQLMaker.normalization.format(minOrdCnt,diffOrdCnt,minTrkitemCnt,diffTrkitemCnt,minRlordAmt,diffRlordAmt))

    normal.registerTempTable("ORD_CNT_RANK_NORMAL")
    ///////////////////////////////////////////////////////////////////

    //val excluding6005 = hiveContext.sql(SQLMaker.excluding6005)
    val excluding6005 = hiveContext.sql(SQLMaker.excluding6005_normal())
    excluding6005.registerTempTable("EXCLUDE_6005")

    val including6005 = hiveContext.sql(SQLMaker.including6005)
    including6005.registerTempTable("INCLUDE_6005")

    val merge = hiveContext.sql(SQLMaker.merge)
    val unionData1 = merge.unionAll(including6005)
    unionData1.registerTempTable("UNION_DATA1")

    //added by Y.G 2017-08-28
    val tvHome = hiveContext.sql(SQLMaker.tvHome)
    val unionData2 = tvHome.unionAll(unionData1)
    unionData2.registerTempTable("UNION_DATA2")

    val siVillage = hiveContext.sql(SQLMaker.siVillage)
    val unionData3 = siVillage.unionAll(unionData2)
    unionData3.registerTempTable("UNION_DATA")


    //20170912 유성 신세계상품권 fadeout 용도(임시)
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
          ,a(16)//createDate
        ))
      .coalesce(1)
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/NORMAL_merge_ALL__" + reader.getDate)

    finalRank
      .map(a => "%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //dispSiteNo
          ,a(2) //sellSiteNo
          ,a(15)//finalRank
          ,a(16)))//createDate
      .coalesce(1)
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/NORMAL_campaign_" + reader.getDate)

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
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/NORMAL_prod_" + reader.getDate)


    //20170629 유 성
    //as-is로직(정규화 미적용)의 SSG와, 정규화 적용된 로직 merge
    val prodFinal = reader.getProd().unionAll(reader.getProdNormal())

    prodFinal
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
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/FINAL_prod_" + reader.getDate)




  }


}
object BestRankNormal{
  def main(args:Array[String]) {
    val executor = new bestRankNormalExecutor
    executor.init()
    executor.run()
  }
}