package ssg.bestrank

/*
    Author  : Ryu Sung
    Date    : 2017-08-31
    Info    :
*/

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import ssg.util.{Reader}


class bestRankGenBrthExecutor {

  //20170512 3일치 클릭데이터
  private val TRACK_ITEM_DTL_DAY = 3
  private var hiveContext : HiveContext = _
  private var sc : SparkContext = _

  def init() {
    val sparkConf = new SparkConf()
      .setAppName("Recommend_BestRankGenBrtdy")
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
    hiveContext.sql(SQLMaker.ordItem3Days.format(d3)).registerTempTable("PRE_ORD_ITEM_3DAYS")
    /////////////////////////////////

    val preTrackItem = reader.trackDtlGenBrth(TRACK_ITEM_DTL_DAY)
    preTrackItem.registerTempTable("PRE_TRACK")

    val eventDF = reader.event
    eventDF.registerTempTable("EVENT")

    val item = hiveContext.sql(SQLMaker.getItem)
    item.registerTempTable("ITEM")

    val trackItemDtl = hiveContext.sql(SQLMaker.getTrackGenBrtdy)
    trackItemDtl.registerTempTable("TRACK_ITEM_DTL")

    val mbr3 = reader.mbr3
    mbr3.registerTempTable("MBR3")

    val mbrUniq = hiveContext.sql(SQLMaker.MBR_UNIQ)
    mbrUniq.registerTempTable("MBR_UNIQ")

    val preOrdItemGenBrtdy = hiveContext.sql(SQLMaker.preOrdItem)
    preOrdItemGenBrtdy.registerTempTable("PRE_ORD_ITEM")

    val ordItemCnt = hiveContext.sql(SQLMaker.getOrdItemGenBrtdyCnt())
    ordItemCnt.registerTempTable("ORD_ITEM_CNT")

    val trkItemCnt = hiveContext.sql(SQLMaker.getTrkItemGenBrtdyCnt)
    trkItemCnt.registerTempTable("TRACK_ITEM_CNT")

    val eventCnt = hiveContext.sql(SQLMaker.getEventCnt)
    eventCnt.registerTempTable("EVENT_CNT")

    val union = hiveContext.sql(SQLMaker.ordTrkGenBrtdyUnion())
    union.registerTempTable("ORD_TRK_UNION")

    ////////////////////////////////////////////////////////////////////////////////
    val itemOrdTrackRaw = hiveContext.sql(SQLMaker.ITEM_ORD_TRK_RAW())
    itemOrdTrackRaw.registerTempTable("ITEM_ORD_TRK_RAW")

    hiveContext.cacheTable("ITEM_ORD_TRK_RAW")

    val itemOrdTrackGenBrtdy = hiveContext.sql(SQLMaker.ITEM_ORD_TRK_GENBRTDY())
    itemOrdTrackGenBrtdy.registerTempTable("ITEM_ORD_TRK_GENBRTDY")

    val itemOrdTrackGen = hiveContext.sql(SQLMaker.ITEM_ORD_TRK_GEN())
    itemOrdTrackGen.registerTempTable("ITEM_ORD_TRK_GEN")

    val itemOrdTrackBrtdy = hiveContext.sql(SQLMaker.ITEM_ORD_TRK_BRTDY())
    itemOrdTrackBrtdy.registerTempTable("ITEM_ORD_TRK_BRTDY")

    val itemOrdTrack = hiveContext.sql(SQLMaker.ITEM_ORD_TRK())
    itemOrdTrack.registerTempTable("ITEM_ORD_TRK")

    ////////////////////////////////////////////////////////////////////////////////
    hiveContext.sql(SQLMaker.LOG_SCALE_DYNAMIC.format("ITEM_ORD_TRK_GENBRTDY")).registerTempTable("LOG_SCALE_GENBRTDY")
    hiveContext.sql(SQLMaker.LOG_SCALE_DYNAMIC.format("ITEM_ORD_TRK_GEN")).registerTempTable("LOG_SCALE_GEN")
    hiveContext.sql(SQLMaker.LOG_SCALE_DYNAMIC.format("ITEM_ORD_TRK_BRTDY")).registerTempTable("LOG_SCALE_BRTDY")
    hiveContext.sql(SQLMaker.LOG_SCALE_DYNAMIC.format("ITEM_ORD_TRK")).registerTempTable("LOG_SCALE")


    hiveContext.cacheTable("LOG_SCALE_GENBRTDY")
    hiveContext.cacheTable("LOG_SCALE_GEN")
    hiveContext.cacheTable("LOG_SCALE_BRTDY")
    hiveContext.cacheTable("LOG_SCALE")


    //정규화를 위해, 로그 값에서 각 feature별 max min값 구하기
    val normal_var_genbrtdy = hiveContext.sql(SQLMaker.minMax.format("LOG_SCALE_GENBRTDY")).head()
    val normal_var_gen = hiveContext.sql(SQLMaker.minMax.format("LOG_SCALE_GEN")).head()
    val normal_var_brth = hiveContext.sql(SQLMaker.minMax.format("LOG_SCALE_BRTDY")).head()
    val normal_var = hiveContext.sql(SQLMaker.minMax.format("LOG_SCALE")).head()

    // 성별 연령별
    val maxOrdCnt_genbrtdy       =  normal_var_genbrtdy(0).asInstanceOf[Float]
    val minOrdCnt_genbrtdy       =  normal_var_genbrtdy(1).asInstanceOf[Float]
    val maxTrkItemCnt_genbrtdy   =  normal_var_genbrtdy(2).asInstanceOf[Float]
    val minTrkItemCnt_genbrtdy   =  normal_var_genbrtdy(3).asInstanceOf[Float]
    val maxRlordAmt_genbrtdy     =  normal_var_genbrtdy(4).asInstanceOf[Float]
    val minRlordAmt_genbrtdy     =  normal_var_genbrtdy(5).asInstanceOf[Float]
    val diffOrdCnt_genbrtdy      =  maxOrdCnt_genbrtdy     - minOrdCnt_genbrtdy
    val diffTrkItemCnt_genbrtdy  =  maxTrkItemCnt_genbrtdy - minTrkItemCnt_genbrtdy
    val diffRlordAmt_genbrtdy    =  maxRlordAmt_genbrtdy   - minRlordAmt_genbrtdy

    // 성별
    val maxOrdCnt_gen       =  normal_var_gen(0).asInstanceOf[Float]
    val minOrdCnt_gen       =  normal_var_gen(1).asInstanceOf[Float]
    val maxTrkItemCnt_gen   =  normal_var_gen(2).asInstanceOf[Float]
    val minTrkItemCnt_gen   =  normal_var_gen(3).asInstanceOf[Float]
    val maxRlordAmt_gen     =  normal_var_gen(4).asInstanceOf[Float]
    val minRlordAmt_gen     =  normal_var_gen(5).asInstanceOf[Float]
    val diffOrdCnt_gen      =  maxOrdCnt_gen     - minOrdCnt_gen
    val diffTrkItemCnt_gen  =  maxTrkItemCnt_gen - minTrkItemCnt_gen
    val diffRlordAmt_gen    =  maxRlordAmt_gen   - minRlordAmt_gen

    // 연령별
    val maxOrdCnt_brth       =  normal_var_brth(0).asInstanceOf[Float]
    val minOrdCnt_brth       =  normal_var_brth(1).asInstanceOf[Float]
    val maxTrkItemCnt_brth   =  normal_var_brth(2).asInstanceOf[Float]
    val minTrkItemCnt_brth   =  normal_var_brth(3).asInstanceOf[Float]
    val maxRlordAmt_brth     =  normal_var_brth(4).asInstanceOf[Float]
    val minRlordAmt_brth     =  normal_var_brth(5).asInstanceOf[Float]
    val diffOrdCnt_brth      =  maxOrdCnt_brth     - minOrdCnt_brth
    val diffTrkItemCnt_brth  =  maxTrkItemCnt_brth - minTrkItemCnt_brth
    val diffRlordAmt_brth    =  maxRlordAmt_brth   - minRlordAmt_brth

    // 전체
    val maxOrdCnt       =  normal_var(0).asInstanceOf[Float]
    val minOrdCnt       =  normal_var(1).asInstanceOf[Float]
    val maxTrkItemCnt   =  normal_var(2).asInstanceOf[Float]
    val minTrkItemCnt   =  normal_var(3).asInstanceOf[Float]
    val maxRlordAmt      =  normal_var(4).asInstanceOf[Float]
    val minRlordAmt      =  normal_var(5).asInstanceOf[Float]
    val diffOrdCnt      = maxOrdCnt     - minOrdCnt
    val diffTrkItemCnt  = maxTrkItemCnt - minTrkItemCnt
    val diffRlordAmt     = maxRlordAmt    - minRlordAmt

    println("================성별 연령별======================")
    println("maxOrdCnt_genbrtdy    : " + maxOrdCnt_genbrtdy   )
    println("minOrdCnt_genbrtdy    : " + minOrdCnt_genbrtdy   )
    println("=================================================")
    println("maxTrkItemCnt_genbrtdy    : " + maxTrkItemCnt_genbrtdy   )
    println("minTrkItemCnt_genbrtdy    : " + minTrkItemCnt_genbrtdy   )
    println("=================================================")
    println("maxRlordAmt_genbrtdy    : " + maxRlordAmt_genbrtdy   )
    println("minRlordAmt_genbrtdy    : " + minRlordAmt_genbrtdy   )
    println("=================================================")
    println("diffOrdCnt_genbrtdy      : " + diffOrdCnt_genbrtdy    )
    println("diffTrkItemCnt_genbrtdy  : " + diffTrkItemCnt_genbrtdy    )
    println("diffRlordAmt_genbrtdy     : " + diffRlordAmt_genbrtdy    )
    println("=================================================")

    println("===================성별==========================")
    println("maxOrdCnt_gen    : " + maxOrdCnt_gen   )
    println("minOrdCnt_gen    : " + minOrdCnt_gen   )
    println("=================================================")
    println("maxTrkItemCnt_gen    : " + maxTrkItemCnt_gen   )
    println("minTrkItemCnt_gen    : " + minTrkItemCnt_gen   )
    println("=================================================")
    println("maxRlordAmt_gen    : " + maxRlordAmt_gen   )
    println("minRlordAmt_gen    : " + minRlordAmt_gen   )
    println("=================================================")
    println("diffOrdCnt_gen      : " + diffOrdCnt_gen    )
    println("diffTrkItemCnt_gen  : " + diffTrkItemCnt_gen    )
    println("diffRlordAmt_gen     : " + diffRlordAmt_gen    )
    println("=================================================")

    println("===================연령별========================")
    println("maxOrdCnt_brth    : " + maxOrdCnt_brth   )
    println("minOrdCnt_brth    : " + minOrdCnt_brth   )
    println("=================================================")
    println("maxTrkItemCnt_brth    : " + maxTrkItemCnt_brth   )
    println("minTrkItemCnt_brth    : " + minTrkItemCnt_brth   )
    println("=================================================")
    println("maxRlordAmt_brth    : " + maxRlordAmt_brth   )
    println("minRlordAmt_brth    : " + minRlordAmt_brth   )
    println("=================================================")
    println("diffOrdCnt_brth      : " + diffOrdCnt_brth    )
    println("diffTrkItemCnt_brth  : " + diffTrkItemCnt_brth    )
    println("diffRlordAmt_brth     : " + diffRlordAmt_brth    )
    println("=================================================")

    println("=====================전체========================")
    println("maxOrdCnt    : " + maxOrdCnt   )
    println("minOrdCnt    : " + minOrdCnt   )
    println("=================================================")
    println("maxTrkItemCnt    : " + maxTrkItemCnt   )
    println("minTrkItemCnt    : " + minTrkItemCnt   )
    println("=================================================")
    println("maxRlordAmt    : " + maxRlordAmt   )
    println("minRlordAmt    : " + minRlordAmt   )
    println("=================================================")
    println("diffOrdCnt      : " + diffOrdCnt    )
    println("diffTrkItemCnt  : " + diffTrkItemCnt    )
    println("diffRlordAmt     : " + diffRlordAmt    )
    println("=================================================")

    //정규화
    val normal_genbrtdy = hiveContext.sql(SQLMaker.normalization_dynamic.format(minOrdCnt,diffOrdCnt,minTrkItemCnt,diffTrkItemCnt,minRlordAmt,diffRlordAmt,"LOG_SCALE_GENBRTDY"))
    val normal_gen = hiveContext.sql(SQLMaker.normalization_dynamic.format(minOrdCnt,diffOrdCnt,minTrkItemCnt,diffTrkItemCnt,minRlordAmt,diffRlordAmt,"LOG_SCALE_GEN"))
    val normal_brth = hiveContext.sql(SQLMaker.normalization_dynamic.format(minOrdCnt,diffOrdCnt,minTrkItemCnt,diffTrkItemCnt,minRlordAmt,diffRlordAmt,"LOG_SCALE_BRTDY"))
    val normal = hiveContext.sql(SQLMaker.normalization_dynamic.format(minOrdCnt,diffOrdCnt,minTrkItemCnt,diffTrkItemCnt,minRlordAmt,diffRlordAmt,"LOG_SCALE"))

    normal_genbrtdy.registerTempTable("ORD_CNT_RANK_NORMAL_GENBRTDY")
    normal_gen.registerTempTable("ORD_CNT_RANK_NORMAL_GEN")
    normal_brth.registerTempTable("ORD_CNT_RANK_NORMAL_BRTDY")
    normal.registerTempTable("ORD_CNT_RANK_NORMAL")
    ///////////////////////////////////////////////////////////////////

    val totalScore_genbrth =  hiveContext.sql(SQLMaker.TOTAL_SCORE.format("ORD_CNT_RANK_NORMAL_GENBRTDY"))
    val totalScore_gen =  hiveContext.sql(SQLMaker.TOTAL_SCORE.format("ORD_CNT_RANK_NORMAL_GEN"))
    val totalScore_brth =  hiveContext.sql(SQLMaker.TOTAL_SCORE.format("ORD_CNT_RANK_NORMAL_BRTDY"))
    val totalScore =  hiveContext.sql(SQLMaker.TOTAL_SCORE.format("ORD_CNT_RANK_NORMAL"))

    totalScore_genbrth.registerTempTable("TOTAL_SCORE_GENBRTDY")
    totalScore_gen.registerTempTable("TOTAL_SCORE_GEN")
    totalScore_brth.registerTempTable("TOTAL_SCORE_BRTDY")
    totalScore.registerTempTable("TOTAL_SCORE")

    val copy6009_genbrth =  hiveContext.sql(SQLMaker.COPY_SITE_AS_6004.format("TOTAL_SCORE_GENBRTDY", "6009"))
    val copy6009_gen =  hiveContext.sql(SQLMaker.COPY_SITE_AS_6004.format("TOTAL_SCORE_GEN", "6009"))
    val copy6009_brth =  hiveContext.sql(SQLMaker.COPY_SITE_AS_6004.format("TOTAL_SCORE_BRTDY", "6009"))
    val copy6009 =  hiveContext.sql(SQLMaker.COPY_SITE_AS_6004.format("TOTAL_SCORE", "6009"))

    val copy6300_genbrth =  hiveContext.sql(SQLMaker.COPY_SITE_AS_6004.format("TOTAL_SCORE_GENBRTDY", "6300"))
    val copy6300_gen =  hiveContext.sql(SQLMaker.COPY_SITE_AS_6004.format("TOTAL_SCORE_GEN", "6300"))
    val copy6300_brth =  hiveContext.sql(SQLMaker.COPY_SITE_AS_6004.format("TOTAL_SCORE_BRTDY", "6300"))
    val copy6300 =  hiveContext.sql(SQLMaker.COPY_SITE_AS_6004.format("TOTAL_SCORE", "6300"))

    val unionData_genbrth = totalScore_genbrth.unionAll(copy6009_genbrth).unionAll(copy6300_genbrth)
    val unionData_gen = totalScore_gen.unionAll(copy6009_gen).unionAll(copy6300_gen)
    val unionData_brth = totalScore_brth.unionAll(copy6009_brth).unionAll(copy6300_brth)
    val unionData = totalScore.unionAll(copy6009).unionAll(copy6300)

    unionData_genbrth.registerTempTable("UNION_DATA_GENBRTDY")
    unionData_gen.registerTempTable("UNION_DATA_GEN")
    unionData_brth.registerTempTable("UNION_DATA_BRTDY")
    unionData.registerTempTable("UNION_DATA")

    val finalRankGenBrth = hiveContext.sql(SQLMaker.FINAL_RANK.format("PARTITION BY siteNo, gen_div_cd, brtdy", "PARTITION BY siteNo, gen_div_cd, brtdy", "UNION_DATA_GENBRTDY" ))
    val finalRankGen = hiveContext.sql(SQLMaker.FINAL_RANK.format("PARTITION BY siteNo, gen_div_cd", "PARTITION BY siteNo, gen_div_cd", "UNION_DATA_GEN" ))
    val finalRankBrth = hiveContext.sql(SQLMaker.FINAL_RANK.format("PARTITION BY siteNo, brtdy", "PARTITION BY siteNo, brtdy", "UNION_DATA_BRTDY" ))
    val finalRank = hiveContext.sql(SQLMaker.FINAL_RANK.format("PARTITION BY siteNo", "PARTITION BY siteNo", "UNION_DATA" ))

    finalRankGenBrth.registerTempTable("FINAL_RANK_GENBRTDY")
    finalRankGen.registerTempTable("FINAL_RANK_GEN")
    finalRankBrth.registerTempTable("FINAL_RANK_BRTDY")
    finalRank.registerTempTable("FINAL_RANK")

    hiveContext.cacheTable("FINAL_RANK_GENBRTDY")
    hiveContext.cacheTable("FINAL_RANK_GEN")
    hiveContext.cacheTable("FINAL_RANK_BRTDY")
    hiveContext.cacheTable("FINAL_RANK")


    // 전체 유니크 아이템아이디 추출
    hiveContext.sql(SQLMaker.getDistcintItemId.format("FINAL_RANK_GENBRTDY"))
      .unionAll(hiveContext.sql(SQLMaker.getDistcintItemId.format("FINAL_RANK_GEN")))
      .unionAll(hiveContext.sql(SQLMaker.getDistcintItemId.format("FINAL_RANK_BRTDY")))
      .unionAll(hiveContext.sql(SQLMaker.getDistcintItemId.format("FINAL_RANK")))
      .registerTempTable("unionItemId")

    hiveContext.sql(SQLMaker.getDistcintItemId.format("unionItemId")).registerTempTable("distinctItem")


    // gen_div_cd
    // 10 : 남성 : MALE
    // 20 : 여성 : FMALE
    hiveContext.sql(SQLMaker.getRankByBrtdy.format("TWET_ALL_GEN_EVAL_SCR","20")).registerTempTable("FINAL_RANK_BRTDY_20")
    hiveContext.sql(SQLMaker.getRankByBrtdy.format("THIT_ALL_GEN_EVAL_SCR","30")).registerTempTable("FINAL_RANK_BRTDY_30")
    hiveContext.sql(SQLMaker.getRankByBrtdy.format("FORT_ALL_GEN_EVAL_SCR","40")).registerTempTable("FINAL_RANK_BRTDY_40")
    hiveContext.sql(SQLMaker.getRankByBrtdy.format("FIFT_ALL_GEN_EVAL_SCR","50")).registerTempTable("FINAL_RANK_BRTDY_50")

    hiveContext.sql(SQLMaker.getRankByGenBrtdy.format("TWET_MALE_EVAL_SCR","10","20")).registerTempTable("FINAL_RANK_GEN_10_BRTDY_20")
    hiveContext.sql(SQLMaker.getRankByGenBrtdy.format("THIT_MALE_EVAL_SCR","10","30")).registerTempTable("FINAL_RANK_GEN_10_BRTDY_30")
    hiveContext.sql(SQLMaker.getRankByGenBrtdy.format("FORT_MALE_EVAL_SCR","10","40")).registerTempTable("FINAL_RANK_GEN_10_BRTDY_40")
    hiveContext.sql(SQLMaker.getRankByGenBrtdy.format("FIFT_MALE_EVAL_SCR","10","50")).registerTempTable("FINAL_RANK_GEN_10_BRTDY_50")

    hiveContext.sql(SQLMaker.getRankByGenBrtdy.format("TWET_FMALE_EVAL_SCR","20","20")).registerTempTable("FINAL_RANK_GEN_20_BRTDY_20")
    hiveContext.sql(SQLMaker.getRankByGenBrtdy.format("THIT_FMALE_EVAL_SCR","20","30")).registerTempTable("FINAL_RANK_GEN_20_BRTDY_30")
    hiveContext.sql(SQLMaker.getRankByGenBrtdy.format("FORT_FMALE_EVAL_SCR","20","40")).registerTempTable("FINAL_RANK_GEN_20_BRTDY_40")
    hiveContext.sql(SQLMaker.getRankByGenBrtdy.format("FIFT_FMALE_EVAL_SCR","20","50")).registerTempTable("FINAL_RANK_GEN_20_BRTDY_50")

    hiveContext.sql(SQLMaker.getRankByGen.format("ALL_AGEGRP_MALE_EVAL_SCR","10")).registerTempTable("FINAL_RANK_GEN_10")
    hiveContext.sql(SQLMaker.getRankByGen.format("ALL_AGEGRP_FMALE_EVAL_SCR","20")).registerTempTable("FINAL_RANK_GEN_20")

    hiveContext.sql(SQLMaker.getRankByAllAll.format("ALL_AGEGRP_GEN_EVAL_SCR")).registerTempTable("FINAL_RANK_ALL_ALL")


    hiveContext.cacheTable("FINAL_RANK_BRTDY_20")
    hiveContext.cacheTable("FINAL_RANK_BRTDY_30")
    hiveContext.cacheTable("FINAL_RANK_BRTDY_40")
    hiveContext.cacheTable("FINAL_RANK_BRTDY_50")

    hiveContext.cacheTable("FINAL_RANK_GEN_10_BRTDY_20")
    hiveContext.cacheTable("FINAL_RANK_GEN_10_BRTDY_30")
    hiveContext.cacheTable("FINAL_RANK_GEN_10_BRTDY_40")
    hiveContext.cacheTable("FINAL_RANK_GEN_10_BRTDY_50")

    hiveContext.cacheTable("FINAL_RANK_GEN_20_BRTDY_20")
    hiveContext.cacheTable("FINAL_RANK_GEN_20_BRTDY_30")
    hiveContext.cacheTable("FINAL_RANK_GEN_20_BRTDY_40")
    hiveContext.cacheTable("FINAL_RANK_GEN_20_BRTDY_50")

    hiveContext.cacheTable("FINAL_RANK_GEN_10")
    hiveContext.cacheTable("FINAL_RANK_GEN_20")

    hiveContext.cacheTable("FINAL_RANK_ALL_ALL")


    val step01 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_01)

    step01
      .map(a => "%s,%s,%s,%s,%s"
        .format(
          a(0) //item
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3) //brandId
          ,a(4)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_01")


    reader.step01Reader().registerTempTable("FINAL_RANK_STEP_01")

    val step02 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_02)

    step02
      .map(a => "%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3)
          ,a(4)
          ,a(5)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_02")

    reader.step02Reader().registerTempTable("FINAL_RANK_STEP_02")

    val step03 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_03)


    step03
      .map(a => "%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3)
          ,a(4)
          ,a(5)
          ,a(6)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_03")

    reader.step03Reader().registerTempTable("FINAL_RANK_STEP_03")

    val step04 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_04)

    step04
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3)
          ,a(4)
          ,a(5)
          ,a(6)
          ,a(7)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_04")

    reader.step04Reader().registerTempTable("FINAL_RANK_STEP_04")

    val step05 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_05)

    step05
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3)
          ,a(4)
          ,a(5)
          ,a(6)
          ,a(7)
          ,a(8)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_05")

    reader.step05Reader().registerTempTable("FINAL_RANK_STEP_05")

    val step06 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_06)

    step06
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3)
          ,a(4)
          ,a(5)
          ,a(6)
          ,a(7)
          ,a(8)
          ,a(9)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_06")

    reader.step06Reader().registerTempTable("FINAL_RANK_STEP_06")

    val step07 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_07)

    step07
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3)
          ,a(4)
          ,a(5)
          ,a(6)
          ,a(7)
          ,a(8)
          ,a(9)
          ,a(10)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_07")

    reader.step07Reader().registerTempTable("FINAL_RANK_STEP_07")

    val step08 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_08)

    step08
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3)
          ,a(4)
          ,a(5)
          ,a(6)
          ,a(7)
          ,a(8)
          ,a(9)
          ,a(10)
          ,a(11)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_08")

    reader.step08Reader().registerTempTable("FINAL_RANK_STEP_08")

    val step09 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_09)

    step09
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3)
          ,a(4)
          ,a(5)
          ,a(6)
          ,a(7)
          ,a(8)
          ,a(9)
          ,a(10)
          ,a(11)
          ,a(12)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_09")

    reader.step09Reader().registerTempTable("FINAL_RANK_STEP_09")

    val step10 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_10)

    step10
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3)
          ,a(4)
          ,a(5)
          ,a(6)
          ,a(7)
          ,a(8)
          ,a(9)
          ,a(10)
          ,a(11)
          ,a(12)
          ,a(13)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_10")

    reader.step10Reader().registerTempTable("FINAL_RANK_STEP_10")

    val step11 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_11)

    step11
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3)
          ,a(4)
          ,a(5)
          ,a(6)
          ,a(7)
          ,a(8)
          ,a(9)
          ,a(10)
          ,a(11)
          ,a(12)
          ,a(13)
          ,a(14)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_11")

    reader.step11Reader().registerTempTable("FINAL_RANK_STEP_11")

    val step12 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_12)

    step12
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3)
          ,a(4)
          ,a(5)
          ,a(6)
          ,a(7)
          ,a(8)
          ,a(9)
          ,a(10)
          ,a(11)
          ,a(12)
          ,a(13)
          ,a(14)
          ,a(15)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_12")

    reader.step12Reader().registerTempTable("FINAL_RANK_STEP_12")

    val step13 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_13)

    step13
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3)
          ,a(4)
          ,a(5)
          ,a(6)
          ,a(7)
          ,a(8)
          ,a(9)
          ,a(10)
          ,a(11)
          ,a(12)
          ,a(13)
          ,a(14)
          ,a(15)
          ,a(16)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_13")

    reader.step13Reader().registerTempTable("FINAL_RANK_STEP_13")

    val step14 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_14)

    step14
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3)
          ,a(4)
          ,a(5)
          ,a(6)
          ,a(7)
          ,a(8)
          ,a(9)
          ,a(10)
          ,a(11)
          ,a(12)
          ,a(13)
          ,a(14)
          ,a(15)
          ,a(16)
          ,a(17)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_14")

    reader.step14Reader().registerTempTable("FINAL_RANK_STEP_14")

    val step15 = hiveContext.sql(SQLMaker.FINAL_RANK_STEP_15)

    step15
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0) //itemId
          ,a(1) //siteNo
          ,a(2) //dispSiteNo
          ,a(3)
          ,a(4)
          ,a(5)
          ,a(6)
          ,a(7)
          ,a(8)
          ,a(9)
          ,a(10)
          ,a(11)
          ,a(12)
          ,a(13)
          ,a(14)
          ,a(15)
          ,a(16)
          ,a(17)
          ,a(18)
        ))
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_15")

    reader.step15Reader().registerTempTable("FINAL_RANK_STEP_15")

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val BDT_BEST100 = hiveContext.sql(SQLMaker.BDT_BEST100)

    BDT_BEST100
      .map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        .format(
          a(0)
          ,a(1)
          ,a(2)
          ,a(3)
          ,a(4)
          ,a(5)
          ,a(6)
          ,a(7)
          ,a(8)
          ,a(9)
          ,a(10)
          ,a(11)
          ,a(12)
          ,a(13)
          ,a(14)
          ,a(15)
          ,a(16)
          ,a(17)
          ,a(18)
          ,a(19)
          ,a(20)
          ,a(21)
        ))
      .coalesce(1)
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/best100GenBrtdy/BDT_BEST100_" + reader.getDate)


  }


}
object BestRankGenBrth{
  def main(args:Array[String]) {
    val executor = new bestRankGenBrthExecutor
    executor.init()
    executor.run()
  }
}