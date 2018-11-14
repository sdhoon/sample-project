package ssg.searchbest

/*
    Author  : Sung Ryu
    Date    : 2015-12-08
    Info    :
*/

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import ssg.util.Reader

class SearchBestExecutor {

  private var hiveContext : HiveContext = _
  private var sc : SparkContext = _


  def init() {
    val sparkConf = new SparkConf()
      .setAppName("Recommend_SearchBest")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.sql.tungsten.enabled", "false")


    sc = new SparkContext(sparkConf)
    hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

  }

  def run(){

    val reader = new Reader(sc, hiveContext)

    //////////////////////// 상품 정보 생성 시작 ///////////////////////////
    val preItem = reader.yesterdayItem //D-2 유니크한 상품 정보(siteNo미포함, 전시시작일 포함)
    preItem.registerTempTable("PRE_ITEM")

    val todayitemPrice = reader.lwstPrice //D-1 사이트별, 영업점별 상품 가격 정보(복수row) - distinct item 후, 마지막 랭킹 결과 만들때 Join 후 D-1일 기준으로 상품 필터링
    todayitemPrice.registerTempTable("TODAY_ITEM_PRICE")

    val itemPrice = reader.yesterdayLwstPrice //D-2 사이트별, 영업점별 상품 가격 정보(복수 row)
    itemPrice.registerTempTable("ITEM_PRICE")

    val distinctItemListDF = hiveContext.sql(SQLMaker.distinctItemList) //D-1 상품 가격 정보에 기반한 distinct 상품 아이디 및 MAX siteNo
    distinctItemListDF.registerTempTable("DISTINCT_ITEM_LIST")

    val fullItemListDF = hiveContext.sql(SQLMaker.fullItemList) //(D-2)사이트,영업점별 상품 종합정보(Item정보에 +상품최신성, 죄저가격, 가격, siteNo 추가됨)(같은 사이트라도 영업점 수 만큼 row중복생성됨)
    fullItemListDF.registerTempTable("FULL_ITEM_LIST")

    val siteUniqItemList = hiveContext.sql(SQLMaker.siteUniqItemList)  //사이트별 유니크한 상품 종합정보(MIN(lwstSellPrc), MAX(open_weight), MIN(dispStrtDts))
    siteUniqItemList.registerTempTable("SITE_UNIQ_ITEM_LIST")

    //////////////////////// 구매이력 생성 시작 ///////////////////////////
    val preOrdItem = reader.ordItem //DW에서 넘어온 8일치 구매정보 Load
    preOrdItem.registerTempTable("ORD_ITEM")

    val itemBuyLogDF = hiveContext.sql(SQLMaker.itemBuyLog) //D-8일 구매 이력정보 생성
    itemBuyLogDF.registerTempTable("ITEM_BUY_LOG")

    val currentItemBuyLogDF = hiveContext.sql(SQLMaker.currentItemBuyLog)   //구매 이력 중, 판매 진행중신 상품에 대한 구매이력으로 추려 저장,
    currentItemBuyLogDF.registerTempTable("CURRENT_ITEM_BUY_LOG")           //사이트,영업점별 상품 정보(FULL_ITEM_LIST와 item_id와 site_no로만 JOIN하기 때문에, 특정 상품의 경우 영업점수 만큼 구매이력 뻥튀기됨

    ///////////////////////// 클릭이력 생성 시작 ///////////////////////////
    val preTrackItemTemp = reader.trackDtl(8)       //D-8일 클릭 이력 추출( D-8부터 current_date까지, 전체 클릭)
    preTrackItemTemp.registerTempTable("TRACKING_LOG_TEMP")

    val preTrackItem = hiveContext.sql(SQLMaker.everyClick)
    preTrackItem.registerTempTable("TRACKING_LOG")

    val cartViewDF = reader.getCartView(8)  //D-8일 장바구니 클릭이력 Load
    cartViewDF.registerTempTable("CART_VIEW")

    val unionClickLogDF = preTrackItem.unionAll(cartViewDF)    //클릭이력과 장바구니 이력 유니온
    unionClickLogDF.registerTempTable("PRE_TRACK")

    val todayLoginInfoDF = hiveContext.sql(SQLMaker.todayLoginInfo)     //D-3부터 D-2어제까지의 클릭 이력에서, 로그인한 사용자의 mbrId와 pcid추출
    todayLoginInfoDF.registerTempTable("TODAY_LOGIN_INFO")

    val loginHistoryDF = reader.loginHistory      //과거 로그인 이력 로드(D-1일에 배치로 생성된 파일)
    loginHistoryDF.registerTempTable("LOGIN_HISTORY")

    val mergeLoginHistoryDF = loginHistoryDF.unionAll(todayLoginInfoDF)  //오늘 이력과 6개월 로그인 이력 머지
    mergeLoginHistoryDF.registerTempTable("MERGE_LOGIN_HISTORY")

    val updateLoginHistoryDF = hiveContext.sql(SQLMaker.updateLoginHistory)     //로그인 이력 갱신 -> pcid를 기준으로 최신updateDate로 유니크하게 남기고, 6개월 이내만 update
    updateLoginHistoryDF.registerTempTable("UPDATE_LOGIN_HISTORY")

    val currentClickLogDF = hiveContext.sql(SQLMaker.currentClickLog) //사이트,영업점별 상품 정보(FULL_ITEM_LIST)와 itemId 로만 JOIN하기 때문에, 백화점 상품은 영업점 수 만큼 구매이력 뻥튀기되고, 2군대에서 동시에 파는 이마트 상품도 두배로 뻥튀기됨
    currentClickLogDF.registerTempTable("CURRENT_CLICK_LOG")

    val eachItemClickLogDF = hiveContext.sql(SQLMaker.eachItemClickLog) // group by 아이템해서 클릭 이력 합
    eachItemClickLogDF.registerTempTable("EACH_ITEM_CLICK_LOG")

    ///////////////////////// 상품별 구매 이력으로 전환 /////////////
    val eachItemBuyLogDF = hiveContext.sql(SQLMaker.eachItemBuyLog)
    eachItemBuyLogDF.registerTempTable("EACH_ITEM_BUY_LOG")

    ///////////////////////// 상품별 클릭,구매 이력 취합 /////////////
    val eachItemLogDF = hiveContext.sql(SQLMaker.eachItemLog)
    eachItemLogDF.registerTempTable("EACH_ITEM_LOG")

    ///////////////////////// 랭킹 로직 시작 /////////////
    // 각 상품별 "구매수" 순위 산정
    val bestOrderCountListDF = hiveContext.sql(SQLMaker.bestOrderCountList)
    bestOrderCountListDF.registerTempTable("BEST_ORDER_COUNT_LIST")

    // 각 상품별 "구매 총액" 순위 산정
    val bestOrderAmtListDF = hiveContext.sql(SQLMaker.bestOrdereAmtList)
    bestOrderAmtListDF.registerTempTable("BEST_ORDER_AMT_LIST")

    // 각 상품별 "클릭" 순위 산정
    val bestClickCountListDF = hiveContext.sql(SQLMaker.bestClickCountList)
    bestClickCountListDF.registerTempTable("BEST_CLICK_COUNT_LIST")
    ////////////////////////////////////////////////////////


    /////////////////////////상품별 랭킹 순위 merge/////////////
    val bestRankListDF = hiveContext.sql(SQLMaker.bestRankList)
    bestRankListDF.registerTempTable("BEST_RANK_LIST")

    /////////////////////////가중치 적용한 최종 결과/////////////
   val dwSrchItemBestDF = hiveContext.sql(SQLMaker.dwSrchItemBest)
    dwSrchItemBestDF.registerTempTable("DW_SRCH_ITEM_BEST")

    val calendar1 = Calendar.getInstance()
    calendar1.add(Calendar.DATE, -1)
    val sd1 = new SimpleDateFormat("yy-MM-dd")

    updateLoginHistoryDF.map(a => "%s,%s,%s".format(
       a(0)     //mbrId
      ,a(1)     //pcid
      ,a(2)     //updateDate
    ))
      .coalesce(1)
      .saveAsTextFile("hdfs://master001p27.prod.moneymall.ssgbi.com:9000/moneymall/recommend/data/LOGIN_HISTORY_" + sd1.format(calendar1.getTime))


    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, 0)
    val sd = new SimpleDateFormat("yyyy-MM-dd")
    println(sd.format(calendar.getTime))


    dwSrchItemBestDF.map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s".format(
           a(0)    //createDate
          ,a(1)    //itemId
          ,a(2)    //dispSiteNo
          ,a(3)    //sellSiteNo
          ,a(4)    //purchaseCount
          ,a(5)    //purchaseAmt
          ,a(6)    //evarScr
          ,a(7)    //convrt
          ,a(8)    //clickMbrId
          ,a(9)    //clickCount
          ,a(10)   //regpeId
          ,a(11)   //regDts
          ,a(12)   //modpeId
          ,a(13)   //modDts
    ))
      .coalesce(1)
      .saveAsTextFile("hdfs://master001p27.prod.moneymall.ssgbi.com:9000/moneymall/recommend/searchBest_" + sd.format(calendar.getTime))


  }
}
object SearchBest{
  def main(args:Array[String]) {
    val executor = new SearchBestExecutor
    executor.init()
    executor.run()
  }
}