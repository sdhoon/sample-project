package ssg.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.datastax.driver.core.utils.UUIDs
import org.apache.hadoop.hive.ql.exec.Utilities.SQLCommand
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import com.datastax.spark.connector._

import scala.collection.immutable.HashMap
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

/**
  * Created by Y.G Chun 14/10/15......
  */
class Reader (sc : SparkContext, hive : HiveContext) extends Serializable{
  private val hiveContext:HiveContext = hive
  private val YARN_HDFS:String = "hdfs://master001p27.prod.moneymall.ssgbi.com:9000"
  private val MRv1_HDFS:String = "hdfs://master001.prod.moneymall.ssgbi.com:9000"

  private val sd = new SimpleDateFormat("yy-MM-dd")
  private val sd2 = new SimpleDateFormat("yyyy-MM-dd")

  import hiveContext.implicits._

  def item() : DataFrame = {
    val item = sc.textFile(YARN_HDFS + "/moneymall/recommend/data/ITEM2.txt")
      .filter(i => !i.contains("ITEM_ID"))
      .map(_.split("\"\\|\""))
      .filter(row => row.length >= 34)
      .map(t => Item(
        t(0).replace("\"", "").trim //itemId
        , t(2).replace("\"", "").trim //mblNm
        , t(4).replace("\"", "").trim //splVenId
        , t(8).replace("\"", "").trim //brandId
        , t(9).replace("\"", "").trim //sellStatCd
        , t(14).replace("\"", "").trim //prodManufCntryId
        , t(15).replace("\"", "").trim  //stdCtgId
        , t(16).replace("\"", "").trim  //itemSellTypeCd
        , t(17).replace("\"", "").trim  //itemSellTypeDtlCd
        , t(21).replace("\"", "").trim  //dispStrtDts
        , t(31).replace("\"", "").trim  //regDts
        , t(33).replace("\"", "").trim  //nitmAplYn
        , t(34).replace("\"", "").trim  //srchPsblYn
      )).toDF()
    item
  }

  //D-1 상품
  def yesterdayItem() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/data/ITEM2_YESTERDAY.txt")
      .filter(i => !i.contains("ITEM_ID"))
      .map(_.split("\"\\|\""))
      //.map(_.split("\\|(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))  // 값안에 컬럼 구분자  | 를 포함하고 있을 시 방어코드
      .filter(row => row.length >= 34)
      .map(t => Item(
        t(0).replace("\"", "").trim //itemId
        , t(2).replace("\"", "").trim //mblNm
        , t(4).replace("\"", "").trim //splVenId
        , t(8).replace("\"", "").trim //brandId
        , t(9).replace("\"", "").trim //sellStatCd
        , t(14).replace("\"", "").trim  //prodManufCntryId
        , t(15).replace("\"", "").trim  //stdCtgId
        , t(16).replace("\"", "").trim  //itemSellTypeCd
        , t(17).replace("\"", "").trim  //itemSellTypeDtlCd
        , t(21).replace("\"", "").trim  //dispStrtDts
        , t(31).replace("\"", "").trim  //regDts
        , t(33).replace("\"", "").trim  //nitmAplYn
        , t(34).replace("\"", "").trim  //srchPsblYn
      )).toDF()

  }

  //표준 카테고
  def stdCtgUTF8 : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/data/STD_CTG_UTF8.txt")
      .filter(s => !s.contains("STD_CTG_ID"))
      .map(_.split("\"\\|\""))
      .filter(row => row.length >= 15)
      .map(s => StdCtgUTF8(
        s(0).replace("\"", "").trim //STD_CTG_ID
        , s(1).replace("\"", "").trim //STD_CTG_NM
        , s(2).replace("\"", "").trim //PRIOR_STD_CTG_ID
        , s(4).replace("\"", "").trim //STD_CTG_LCLS_ID
        , s(5).replace("\"", "").trim //STD_CTG_LCLS_NM
        , s(6).replace("\"", "").trim //STD_CTG_MCLS_ID
        , s(7).replace("\"", "").trim //STD_CTG_MCLS_NM
        , s(8).replace("\"", "").trim //STD_CTG_SCLS_ID
        , s(9).replace("\"", "").trim //STD_CTG_SCLS_NM
        , s(10).replace("\"", "").trim //STD_CTG_DCLS_ID
        , s(11).replace("\"", "").trim //STD_CTG_DCLS_NM
        , s(14).replace("\"", "").trim  //USE_YN
      )).toDF()
  }

  //표준 카테고
  def stdCtg : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/data/STD_CTG.txt")
      .filter(s => !s.contains("STD_CTG_ID"))
      .map(_.split("\"\\|\""))
      .filter(row => row.length >= 15)
      .map(s => StdCtg(
        s(0).replace("\"", "").trim //STD_CTG_ID
        , s(2).replace("\"", "").trim //PRIOR_STD_CTG_ID
        , s(4).replace("\"", "").trim //STD_CTG_LCLS_ID
        , s(6).replace("\"", "").trim //STD_CTG_MCLS_ID
        , s(8).replace("\"", "").trim //STD_CTG_SCLS_ID
        , s(10).replace("\"", "").trim //STD_CTG_DCLS_ID
        , s(14).replace("\"", "").trim  //USE_YN
      )).toDF()
  }

  //상품가격
  def lwstPrice : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/data/ITEM_LWSTPRC.txt")
      .filter(s => !s.contains("ITEM_ID"))
      .map(_.split("\\|"))
      .filter(row => row.length >= 10)
      .map(l => ItemPrice(
        l(0).replace("\"", "").trim //ITEM_ID
        , l(1).replace("\"", "").trim //SITE_NO
        , l(2).replace("\"", "").trim //SALESTR_NO
        , l(3).replace("\"", "").trim //SELLPRC
        , l(4).replace("\"", "").trim)//LEST_SELLPRC
      ).toDF()
  }

  //D-1 상품가격
  def yesterdayLwstPrice : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/data/ITEM_LWSTPRC_YESTERDAY.txt")
      .filter(s => !s.contains("ITEM_ID"))
      .map(_.split("\\|"))
      .filter(row => row.length >= 10)
      .map(l => ItemPrice(
        l(0).replace("\"", "").trim //ITEM_ID
        , l(1).replace("\"", "").trim //SITE_NO
        , l(2).replace("\"", "").trim //SALESTR_NO
        , l(3).replace("\"", "").trim //SELLPRC
        , l(4).replace("\"", "").trim)//LEST_SELLPRC
      ).toDF()
  }

  /*
  * MBR3.txt는 증분 이기 때문에 본격 테스트 하기 전에 전체 데이터 필요함.
  *
  */
  //회원
  def mbr : RDD[Row] = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/data/MBR3.txt")
      .filter(s => !s.contains("MBR_ID"))
      .map(_.split("\\,"))
      .map(r => Row(
        r(0) //MBR_ID
        , r(4)  //MBR_LOGIN_ID
        , r(12) //GEN_DIV_CD
        , r(14)))//BRTDY
  }

  def orderEventRow(date:String) : DataFrame = {

    //println(sd.format(calendar.getTime))
    val orderEventRow = sc.textFile(YARN_HDFS + "/moneymall/tsv/order-event-row/" + date + "/*/*")

    orderEventRow
      .filter(t => t.contains("\t"))
      .map(_.split("\t"))
      .map(t => OrderEventRow(
        t(0),        //ordNo
        t(5),  //itemId
        t(10),   //timestamp
        t(17).toLong //mbrId
      )).toDF()
  }

  //주문정보
  def ordItem : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/data/ORD_ITEM.txt")
      .filter(s => !s.contains("ORORD_NO"))
      .map(_.split("\\|"))
      .filter(row => row.length >= 92)
      .map(r => OrdItem(
        r(0).replace("\"", "").trim //orordNo
        , r(1).replace("\"", "").trim //ordNo
        , r(3).replace("\"", "").trim //ordItemDivCd
        , r(5).replace("\"", "").trim //ordDt
        , r(10).replace("\"", "").trim//dvicDivCd
        , r(17).replace("\"", "").trim//ordItemStatCd
        , r(19).replace("\"", "").trim//itemId
        , r(24).replace("\"", "").trim//siteNo
        , r(25).replace("\"", "").trim//mbrId
        , r(27).replace("\"", "").trim//sellPrc
        , r(28).replace("\"", "").trim//splPrc
        , r(31).replace("\"", "").trim//orordQty
        ,
        {
          //orordAmt
          if(r(41).contains("Z~"))
            r(41)
          else
            r(41).replace("\"", "").trim
        }
        , r(58).replace("\"", "").trim//stdCtgId
        , r(60).replace("\"", "").trim//brandId
        , r(64).replace("\"", "").trim//infloSiteNo
        , r(68).replace("\"", "").trim//splVenId
        , r(92).replace("\"", "").trim//regDts
        , r(42).replace("\"", "").trim.toLong  //ordAmt 20170703 유성 추가 : 주문금액 산정 기준 변경
        , r(47).replace("\"", "").trim.toLong  //owncoBdnItemDcAmt 20170703 유성 추가 : 주문금액 산정 기준 변경
        , r(48).replace("\"", "").trim.toLong  //coopcoBdnItemDcAmt 20170703 유성 추가 : 주문금액 산정 기준 변경
      )

      ).toDF
  }

  def getTrackDataFrame(trackItemDtl : RDD[String]) : DataFrame = {
    trackItemDtl
      .filter(t => t.contains("\t"))
      .map(_.split("\t"))
      .filter(t => t.length >= 65)
      .filter(t => t(7) != "" && t(7).length() == 13)
      .filter(x => x(7).matches("[0-9]{13}"))
      .filter(t =>
        t(4).contains("/itemSimple.ssg")       ||
          t(4).contains("/itemView.ssg")         ||
          t(4).contains("/itemView01.ssg")       ||
          t(4).contains("/itemDtl.ssg")          ||
          t(4).contains("/burberryItemView.ssg") ||
          t(4).contains("/gucciItemDtl.ssg")     ||
          t(17).contains("ItemDetail"))
      .map(t => TrackItemDtl(
        new java.sql.Timestamp(t(0).trim.toLong).toString.substring(0,10)// timeStamp
        , t(2).trim  //IP
        , t(6).trim  //siteNo
        , t(7).trim  //itemId
        , t(28).trim //pcid
        , t(34).trim //mbr_id
        , t(29).trim //fsid
        , t(0).trim //timestamp
      )).toDF()
  }

  def getClick(calendar: Calendar) : DataFrame = {

    val trackItemDtl = sc.textFile(YARN_HDFS + "/moneymall/tsv/every-click-event/" + sd.format(calendar.getTime) + "/*/*")
    getTrackDataFrame(trackItemDtl)
  }

  //에브리클릭
  def trackItemDtl(day:Int) : DataFrame = {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    println("click" + sd.format(calendar.getTime))
    var trackItemDtl = sc.textFile(YARN_HDFS + "/moneymall/tsv/every-click-event/" + sd.format(calendar.getTime) + "/*/*")
    for (i <- 1 to day - 1){
      calendar.add(Calendar.DATE, -1)
      println("click" + sd.format(calendar.getTime))
      trackItemDtl ++= sc.textFile(YARN_HDFS + "/moneymall/tsv/every-click-event/" + sd.format(calendar.getTime) + "/*/*")
    }

    getTrackDataFrame(trackItemDtl)

  }


  //에브리클릭
  def trackDtl(day:Int) : DataFrame = {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    val sd = new SimpleDateFormat("yy-MM-dd")
    println(sd.format(calendar.getTime))
    var trackItemDtl = sc.textFile(YARN_HDFS + "/moneymall/tsv/every-click-event/" + sd.format(calendar.getTime) + "/*")
    for (i <- 1 to day - 1){
      calendar.add(Calendar.DATE, -1)
      println(sd.format(calendar.getTime))
      trackItemDtl ++= sc.textFile(YARN_HDFS + "/moneymall/tsv/every-click-event/" + sd.format(calendar.getTime) + "/*")
    }

    trackItemDtl
      .filter(t => t.contains("\t"))
      .map(_.split("\t"))
      .filter(t => t.length >= 65)
      //.filter(t => t(7) != "" && t(7).length() == 13)
      .filter(t =>
      t(4).contains("/itemSimple.ssg")      ||
        t(4).contains("/itemView.ssg")        ||
        t(4).contains("/itemView01.ssg")      ||
        t(4).contains("/itemDtl.ssg")         ||
        t(4).contains("/burberryItemView.ssg")||
        t(4).contains("/gucciItemDtl.ssg")    ||
        t(17).contains("ItemDetail") )
      .map(t => TrackItemDtl(
        new java.sql.Timestamp(t(0).trim.toLong).toString.substring(0,10)// timeStamp
        , t(2).trim  //IP
        , t(6).trim  //siteNo
        , t(7).trim  //itemId
        , t(28).trim //pcid
        , t(34).trim //mbr_id
        , t(29).trim //fsid
        , t(0).trim //timestamp
      )).toDF()
  }


  //이벤트
  //20170831 유성 split 추가
  def event() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/data/ESPEC_PNSHOP_ITEM.txt")
      .filter(e => !e.contains("ITEM_ID"))
      .map(_.split("\\|"))
      .map(e => ItemEvent(
        e(0).toString() //ITEM_ID
      )).toDF()
  }

  def getFormattedDate(date:Date) : String = {
    val format  = new SimpleDateFormat("yyyy-MM-dd")
    format.format(date)
  }
  def getFormattedTime(date:Date) : String = {
    val hour  = new SimpleDateFormat("HH")
    hour.format(date)
  }

  //D-2일 로그인 이력 불러오기
  def loginHistory : DataFrame = {

    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -2)
    val sd = new SimpleDateFormat("yy-MM-dd")

    sc.textFile(YARN_HDFS + "/moneymall/recommend/data/LOGIN_HISTORY_" + sd.format(calendar.getTime) + "/*")
      .map(_.split(","))
      .map(row => LoginHistory(
        row(0).replace("\"", "").trim
        , row(1).replace("\"", "").trim
        , row(2).replace("\"", "").trim
      )).toDF()
  }


  //장바구니 클릭
  def getCartView(day:Int) : DataFrame = {

    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    println(sd.format(calendar.getTime))
    var cartViewRDD = sc.textFile(YARN_HDFS + "/moneymall/tsv/cart-event-row/" + sd.format(calendar.getTime) + "/*")
    for (i <- 1 to day - 1){
      calendar.add(Calendar.DATE, -1)
      println(sd.format(calendar.getTime))
      cartViewRDD ++= sc.textFile(YARN_HDFS + "/moneymall/tsv/cart-event-row/" + sd.format(calendar.getTime) + "/*")
    }

    cartViewRDD
      .filter(t => t.contains("\t"))
      .map(_.split("\t"))
      .filter(t => t.length >= 11)
      .filter(t => t(7) != "")
      .map(t => CartView(
        new java.sql.Timestamp(t(2).trim.toLong).toString.substring(0,10)// timeStamp
        , ""         //ip
        , t(6).trim  //siteNo
        , t(7).trim  //itemId
        , t(0).trim  //pcId
        , t(10).trim //mbrId
      )).toDF()
  }

  def itemBased(types:String, date:String) : DataFrame = {
    val itemBasedRDD = sc.textFile(YARN_HDFS + "/moneymall/recommend/cf/itembased/output/mahout/" + types + "/" + date + "/*")
    itemBasedRDD
      .filter(t => t.contains(","))
      .map(_.split(","))
      .filter(t => t.length >= 3)
      .map(t => ItemBased(
        t(0).reverse.padTo(13, "0").reverse.mkString,
        t(1).reverse.padTo(13, "0").reverse.mkString,
        t(2).toDouble
      )).toDF()
  }

  def getDate(): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    sdf.format(calendar.getTime)
  }


  //유형별검색베스트
  //에브리클릭 -고도화
  def everyclickInfoD1D8() : DataFrame = {
    println("=========== everyClick Date : =============")
    val calendar = Calendar.getInstance()

    var everyclickInfoD1D8 = sc.makeRDD(Seq(""))

    for (i <- 0 to 2){
      calendar.add(Calendar.DATE, -1)
      println(sd.format(calendar.getTime))
      everyclickInfoD1D8 ++= sc.textFile(YARN_HDFS + "/moneymall/tsv/every-click-event/" + sd.format(calendar.getTime) + "/*")
    }

    calendar.add(Calendar.DATE, -5)
    println(sd.format(calendar.getTime))

    everyclickInfoD1D8 ++= sc.textFile(YARN_HDFS + "/moneymall/tsv/every-click-event/" + sd.format(calendar.getTime) + "/*")

    // 상품ID있는 클릭만 추출
    everyclickInfoD1D8
      .filter(line => line.contains("\t"))
      .map(_.split("\t"))
      .filter(row => row.length >= 65)
      .filter(row => row(7) != "" && row(7).length() == 13)
      .map(row => everyClickSch(
        {
          new java.sql.Timestamp(row(0).trim.toLong).toString.substring(0,10)   // timeStamp
        }
        ,{
          var tmpStr = ""
          if ( row(55) == "0")
            tmpStr = "PC"
          else if(row(22).toLowerCase != "app" && row(22).toLowerCase != "uplus")
            tmpStr = "MW"
          else
            tmpStr = "MA"

          tmpStr
        }         //type
        ,{
          row(4)
        }         //url
        ,row(12)  //search
        ,row(18)  //referrer
        ,row(7)   //itemId
        ,row(13)  //tarea
        ,row(6)   //siteNo
        ,row(17)  //googleCode
      )
      ).toDF()
  }

  //기여매장
  def contSales() : DataFrame = {
    println("=========== contSales Date : =============")

    val calendar = Calendar.getInstance()

    var contSales = sc.makeRDD(Seq(""))

    //D-1, D-2, D-3
    for (i <- 0 to 2){
      calendar.add(Calendar.DATE, -1)
      println(sd.format(calendar.getTime))
      contSales ++= sc.textFile(YARN_HDFS + "/moneymall/tsv/previous-sales/" + sd.format(calendar.getTime) + "/*")
    }

    //D-8
    calendar.add(Calendar.DATE, -5)
    println(sd.format(calendar.getTime))

    contSales ++= sc.textFile(YARN_HDFS + "/moneymall/tsv/previous-sales/" + sd.format(calendar.getTime) + "/*")

    contSales
      .filter(line => line.contains("\t"))
      .map(_.split("\t"))
      .filter(row => row.length >= 71)
      .filter(row => row(1) != "" && row(1).length() == 13)
      .map(row => contSalesSch(
        {
          new java.sql.Timestamp(row(7).trim.toLong).toString.substring(0, 10) // timeStamp
        }
        ,{
          row(11)
        }        //url
        ,row(13) //domainSiteNo
        ,{
          var tmpStr = ""
          if (row(27) == "10")
            tmpStr = "PC"
          else if (row(29).toLowerCase != "app" && row(29).toLowerCase != "uplus")
            tmpStr = "MW"
          else
            tmpStr = "MA"

          tmpStr
        }        //deviceType
        ,row(19) //search
        ,row(25) //referrer
        ,row(1)  //itemId
        ,row(24) //구글코드
        ,row(20) //tarea
      )
      ).toDF()
  }

  def itemReply() : DataFrame = {

    val sd = new SimpleDateFormat("YYYY-MM-dd")
    println("=========== itemReply Date : =============")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -7)
    println(sd.format(calendar.getTime))
    val itemReply = sc.textFile(YARN_HDFS + "/moneymall/recommend/data/POSTING_" + sd.format(calendar.getTime))

    itemReply
      .filter(line => line.contains("\t"))
      .map(_.split("\t"))
      .filter(row => row.length >= 2)
      .filter(row => row(0) != "" && row(0).length() == 13)
      .map(row => itemReplySch(
        row(0)         // itmeId
        ,row(1).toInt   // replyCnt
      )
      ).toDF()

  }

  //리액팅로그
  def reactingEventD1D8() : DataFrame = {
    println("=========== reactingLog Date : =============")
    val calendar = Calendar.getInstance()

    var reactingEventD1D8 = sc.makeRDD(Seq(""))

    for (i <- 0 to 2){
      calendar.add(Calendar.DATE, -1)
      println(sd.format(calendar.getTime))
      reactingEventD1D8 ++= sc.textFile(YARN_HDFS + "/moneymall/tsv/reacting-event/" + sd.format(calendar.getTime) + "/*")
    }

    calendar.add(Calendar.DATE, -5)
    println(sd.format(calendar.getTime))

    reactingEventD1D8 ++= sc.textFile(YARN_HDFS + "/moneymall/tsv/reacting-event/" + sd.format(calendar.getTime) + "/*")

    // 상품ID있는 클릭만 추출
    reactingEventD1D8
      .filter(line => line.contains("\t"))
      .map(_.split("\t"))
      .filter(row => row.length >= 65)
      .filter(row => row(7) != "" && row(7).length() == 13)
      .map(row => everyClickSch(
        {
          new java.sql.Timestamp(row(0).trim.toLong).toString.substring(0,10)   // timeStamp
        }
        ,{
          "MA"
        }         //type
        ,{
          row(4)
        }         //url
        ,row(12)  //search
        ,row(18)  //referrer
        ,row(7)   //itemId
        ,row(13)  //tarea
        ,row(6)   //siteNo
        ,row(17)  //googleCode
      )
      ).toDF()
  }

  //주문정보
  def ordItem2 : DataFrame = {

    sc.textFile(YARN_HDFS + "/moneymall/recommend/data/ORD_ITEM.txt")
      .filter(s => !s.contains("ORORD_NO"))
      .map(_.split("\\|"))
      .filter(row => row.length >= 92)
      .filter(row => row(19) != "" && row(19).length() == 13)
      .map(r => OrdItem2(
        r(0).replace("\"", "").trim            //orordNo
        , r(5).replace("\"", "").trim          //ordDt
        , r(19).replace("\"", "").trim         //itemId
        , r(42).replace("\"", "").trim.toLong  //ordAmt
        , r(47).replace("\"", "").trim.toLong  //owncoBdnItemDcAmt
        , r(48).replace("\"", "").trim.toLong  //coopcoBdnItemDcAmt
        , r(32).replace("\"", "").trim.toInt   //ordQty
        , r(9).replace("\"", "").trim          //ordAplTgtCd
      )).toDF

  }

  def getCategorize(types:String, date:String) : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/cf/itembased/output/categorize/" + types + "/" + date + "/*")
      .map(_.split(","))
      .map(r => CategorizedItemBased(
        r(0), //item_id
        r(4), //stdCtg
        r(1), //recommend_item_id
        r(2), //siteNo
        r(5), //recommendStdCtg
        r(3).toDouble, //score
        types)).toDF
  }


  def getYarnHDFSURI() : String = {
    YARN_HDFS
  }


  def getSearchBestEachType() : DataFrame = {

    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)

    sc.textFile(YARN_HDFS + "/moneymall/recommend/searchBestCaseModel/" + sd2.format(calendar.getTime) + ".allCase")
      .map(_.split(","))
      .map(r => SearchBestCaseModel(
        r(1), //item_id
        r(2), //SRCH_TYPE_FIRS_SCR
        r(3), //SRCH_TYPE_SCND_SCR
        r(4)  //SRCH_TYPE_THRD_SCR
      )).toDF
  }

  def getBrand() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/data/BRAND2_UTF.txt")
      .filter(s => !s.contains("BRAND_REG_MAIN_DIV_CD"))
      .map(_.split("\\|"))
      .map(r => Brand(
        r(0).replace("\"", "").trim, //brandId
        r(1).replace("\"", "").trim, //brandNm
        r(3).replace("\"", "").trim, //useYn
        r(4).replace("\"", "").trim //brandRegMainDivCd
      )).toDF
  }

  def getBestRank() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/merge_ALL__" + getDate)
      .map(_.split(","))
      .map(r => BestRank(
        r(0), //itemId
        r(1), //siteNo
        r(2), //dispSiteNo
        r(3),  //ordCnt
        r(4),  //ordCntRank
        r(5),  //brandId
        r(8),  //trkItemCnt
        r(9),  //trkItemCntRank
        r(10),  //sellPrc
        r(11),  //sellPrcRank
        r(12),  //totalScore
        r(13),  //totalRank
        r(15)  //finalRank
      )).toDF
  }

  def getItemBase() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/itembase/cassandra/*")
      .map(_.split(","))
      .filter(s => !s.contains("item_id"))
      .map(row => Itembase(
        row(0),
        row(1),
        row(2),
        row(3))
      ).toDF
  }

  def getCassandraItemBase() {
    val cassandraItemBase = sc.cassandraTable("recommend","itembase")
                              .select("item_id", "site_no", "recommend_item_id", "reg_dts", "score")

    cassandraItemBase.map( row =>
      checkNull(row.getString("item_id")) + "," +
      checkNull(row.getString("site_no")) + "," +
      checkNull(row.getString("recommend_item_id"))+ "," +
      checkNull(row.getString("reg_dts"))+ "," +
      checkNull(row.getString("score"))
    ).saveAsTextFile(YARN_HDFS + "/moneymall/recommend/cassandra/itembase/")
  }

  def checkNull(value:String): String = {
    val data = "";
    if(value == null){
      data
    }else{
      value
    }
  }

  //20170426 유 성
  def getOfflineItemMpng() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/data/OFFLINE_ITEM_MPNG/CURRENT/*")
      .map(_.split("\\x5c\\x7c"))
      .filter(row => row.length > 1)
      .map(row => OFFLINE_ITEM_MPNG_SCH(
        row(0)
        , row(1)
        , row(2)
      )
      ).toDF()
  }

  def getEmartOffSaleList() : DataFrame = {
    println("=========== EmartOffSaleList Date : =============")
    val calendar = Calendar.getInstance()

    var emartOffSaleListD1D3 = sc.makeRDD(Seq(""))

    calendar.add(Calendar.DATE, -1) // 오프라인 실적은 D-2일부터 시작됨

    var i = 0

    var j = 0

    val conf = new Configuration()

    val hdfs = FileSystem.get(conf)

    while(i < 3 && j < 10)    { // 총 최신 3일치 주문 데이터 가져옴
      calendar.add(Calendar.DATE, -1)

      if(hdfs.isDirectory(new Path("/user/pdw_user/DW/ODS/EMART_OFF_SALE_LIST_MOD/" + sd.format(calendar.getTime))))  {
        println("/user/pdw_user/DW/ODS/EMART_OFF_SALE_LIST_MOD/" + sd.format(calendar.getTime))

        emartOffSaleListD1D3 ++= sc.textFile(YARN_HDFS + "/user/pdw_user/DW/ODS/EMART_OFF_SALE_LIST_MOD/" + sd.format(calendar.getTime) + "/*")

        i=i+1
      }

      j=j+1 //무한loop방지
    }

    emartOffSaleListD1D3
      .map(_.split("\\x5c\\x7c"))
      .filter(row => row.length > 1)
      .map(row => EMART_OFF_SALE_LIST_SCH(
        row(0)
        ,row(7)
      )
      ).toDF()
  }



  def getProd() : DataFrame = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    val today = sdf.format(calendar.getTime)

    sc.textFile(YARN_HDFS + "/moneymall/recommend/prod_" + today)
      .map(_.split(","))
      .filter(r => r(1) == "6005")
      .map(r => prodData_sch(
        r(0)
        ,r(1)
        ,r(2)
        ,r(3)
        ,r(4)
        ,r(5)
        ,r(6)
        ,r(7)
        ,r(8)
        ,r(9)
        ,r(10)
        ,r(11)
        ,r(12)
        ,r(13)
        ,r(14)
        ,r(15)
        ,r(16)
      )
      ).toDF()
  }

  def getProdNormal() : DataFrame = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    val today = sdf.format(calendar.getTime)

    sc.textFile(YARN_HDFS + "/moneymall/recommend/NORMAL_prod_" + today)
      .map(_.split(","))
      .filter(r => r(1) != "6005")
      .map(r => prodData_sch(
        r(0)
        ,r(1)
        ,r(2)
        ,r(3)
        ,r(4)
        ,r(5)
        ,r(6)
        ,r(7)
        ,r(8)
        ,r(9)
        ,r(10)
        ,r(11)
        ,r(12)
        ,r(13)
        ,r(14)
        ,r(15)
        ,r(16)
      )
      ).toDF()
  }




  //20170831 Best100 성별연령별 클릭 데이터
  //에브리클릭
  def trackDtlGenBrth(day:Int) : DataFrame = {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    val sd = new SimpleDateFormat("yy-MM-dd")
    println(sd.format(calendar.getTime))
    var trackItemDtlGenBrth = sc.textFile(YARN_HDFS + "/moneymall/tsv/every-click-event/" + sd.format(calendar.getTime) + "/*")
    for (i <- 1 to day - 1){
      calendar.add(Calendar.DATE, -1)
      println(sd.format(calendar.getTime))
      trackItemDtlGenBrth ++= sc.textFile(YARN_HDFS + "/moneymall/tsv/every-click-event/" + sd.format(calendar.getTime) + "/*")
    }

    trackItemDtlGenBrth
      .filter(t => t.contains("\t"))
      .map(_.split("\t"))
      .filter(t => t.length >= 65)
      .map(t => TrackItemDtlGenBrtdy(
          new java.sql.Timestamp(t(0).trim.toLong).toString.substring(0,10)// timeStamp
          , t(2).trim  //IP
          , t(6).trim  //siteNo
          , t(7).trim  //itemId
          , t(28).trim //pcid
          , t(34).trim //mbr_id
          , t(29).trim //fsid
          , t(0).trim //timestamp
          , {
            var gen_div_cd = t(52).trim //gender

            if(gen_div_cd == "1")
              "10"
            else if(gen_div_cd == "2")
              "20"
            else
              ""

          }
          , {
            if(t(53).length < 1)    {
              ""
            } else {

              val age:Int = t(53).toInt

              if(age > 0 && age < 30)    {
                "20"
              }else if(age >= 30 && age < 40) {
                "30"
              }else if(age >= 40 && age < 50) {
                "40"
              }else if(age >= 50 && age <= 100) {
                "50"
              }else   {
                ""
              }
            }
          }//age
      )).toDF()
  }


  def mbr3() : DataFrame = {
    sc.textFile(YARN_HDFS + "/tmp/MBR3")
      .filter(i => !i.contains("MBR_ID"))
      .map(_.split("\t"))
      .filter(row => row.length >= 37)
      .map(t => mbr_schema(
        t(0).replace("\"", "").trim //mbr_id
        , t(1).replace("\"", "").trim //mbr_stat_cd
        , t(3).replace("\"", "").trim //mbr_type_cd
        , t(12).replace("*", "").trim //gen_div_cd
        , {
          if(t(14).length < 4)    {
            ""
          } else {
            val calendar = Calendar.getInstance()
            val sdYear = new SimpleDateFormat("yyyy")

            val year:Int = sdYear.format(calendar.getTime).toInt

            val age:Int = t(14).substring(0,4).toInt

            val agediff:Int = year - age

            if(agediff >= 0 && agediff < 20)    {
              "20"
            }else if(agediff >= 20 && agediff < 30) {
              "20"
            }else if(agediff >= 30 && agediff < 40) {
              "30"
            }else if(agediff >= 40 && agediff < 50) {
              "40"
            }else if(agediff >= 50 && agediff <= 100) {
              "50"
            }else   {
              ""
            }
          }
        }//age
      )).toDF()
  }

  def step01Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_01")
      .map(_.split(","))
      .map(e => step01_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
      )).toDF()
  }

  def step02Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_02")
      .map(_.split(","))
      .map(e => step02_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
        ,e(5).toString()
      )).toDF()
  }


  def step03Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_03")
      .map(_.split(","))
      .map(e => step03_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
        ,e(5).toString()
        ,e(6).toString()
      )).toDF()
  }

  def step04Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_04")
      .map(_.split(","))
      .map(e => step04_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
        ,e(5).toString()
        ,e(6).toString()
        ,e(7).toString()
      )).toDF()
  }

  def step05Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_05")
      .map(_.split(","))
      .map(e => step05_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
        ,e(5).toString()
        ,e(6).toString()
        ,e(7).toString()
        ,e(8).toString()
      )).toDF()
  }

  def step06Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_06")
      .map(_.split(","))
      .map(e => step06_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
        ,e(5).toString()
        ,e(6).toString()
        ,e(7).toString()
        ,e(8).toString()
        ,e(9).toString()
      )).toDF()
  }

  def step07Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_07")
      .map(_.split(","))
      .map(e => step07_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
        ,e(5).toString()
        ,e(6).toString()
        ,e(7).toString()
        ,e(8).toString()
        ,e(9).toString()
        ,e(10).toString()
      )).toDF()
  }

  def step08Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_08")
      .map(_.split(","))
      .map(e => step08_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
        ,e(5).toString()
        ,e(6).toString()
        ,e(7).toString()
        ,e(8).toString()
        ,e(9).toString()
        ,e(10).toString()
        ,e(11).toString()
      )).toDF()
  }

  def step09Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_09")
      .map(_.split(","))
      .map(e => step09_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
        ,e(5).toString()
        ,e(6).toString()
        ,e(7).toString()
        ,e(8).toString()
        ,e(9).toString()
        ,e(10).toString()
        ,e(11).toString()
        ,e(12).toString()
      )).toDF()
  }

  def step10Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_10")
      .map(_.split(","))
      .map(e => step10_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
        ,e(5).toString()
        ,e(6).toString()
        ,e(7).toString()
        ,e(8).toString()
        ,e(9).toString()
        ,e(10).toString()
        ,e(11).toString()
        ,e(12).toString()
        ,e(13).toString()
      )).toDF()
  }

  def step11Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_11")
      .map(_.split(","))
      .map(e => step11_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
        ,e(5).toString()
        ,e(6).toString()
        ,e(7).toString()
        ,e(8).toString()
        ,e(9).toString()
        ,e(10).toString()
        ,e(11).toString()
        ,e(12).toString()
        ,e(13).toString()
        ,e(14).toString()
      )).toDF()
  }

  def step12Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_12")
      .map(_.split(","))
      .map(e => step12_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
        ,e(5).toString()
        ,e(6).toString()
        ,e(7).toString()
        ,e(8).toString()
        ,e(9).toString()
        ,e(10).toString()
        ,e(11).toString()
        ,e(12).toString()
        ,e(13).toString()
        ,e(14).toString()
        ,e(15).toString()
      )).toDF()
  }

  def step13Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_13")
      .map(_.split(","))
      .map(e => step13_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
        ,e(5).toString()
        ,e(6).toString()
        ,e(7).toString()
        ,e(8).toString()
        ,e(9).toString()
        ,e(10).toString()
        ,e(11).toString()
        ,e(12).toString()
        ,e(13).toString()
        ,e(14).toString()
        ,e(15).toString()
        ,e(16).toString()
      )).toDF()
  }

  def step14Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_14")
      .map(_.split(","))
      .map(e => step14_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
        ,e(5).toString()
        ,e(6).toString()
        ,e(7).toString()
        ,e(8).toString()
        ,e(9).toString()
        ,e(10).toString()
        ,e(11).toString()
        ,e(12).toString()
        ,e(13).toString()
        ,e(14).toString()
        ,e(15).toString()
        ,e(16).toString()
        ,e(17).toString()
      )).toDF()
  }
  def step15Reader() : DataFrame = {
    sc.textFile(YARN_HDFS + "/moneymall/recommend/best100GenBrtdy/tmp/best100GenBrtdy.FINAL_RANK_STEP_15")
      .map(_.split(","))
      .map(e => step15_schema(
        e(0).toString()
        ,e(1).toString()
        ,e(2).toString()
        ,e(3).toString()
        ,e(4).toString()
        ,e(5).toString()
        ,e(6).toString()
        ,e(7).toString()
        ,e(8).toString()
        ,e(9).toString()
        ,e(10).toString()
        ,e(11).toString()
        ,e(12).toString()
        ,e(13).toString()
        ,e(14).toString()
        ,e(15).toString()
        ,e(16).toString()
        ,e(17).toString()
        ,e(18).toString()
      )).toDF()
  }



}
