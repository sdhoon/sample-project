// Donghoon Seo
// Spark 1.6 Version
// 2016-01-15

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util._
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer

object ContSales {

// sc initialize
val sparkConf = new SparkConf().setAppName("ContSales")
                                .set("spark.hadoop.validateOutputSpecs", "false")
                                .set("spark.local.dir","/data01/spark_tmp/")
                                //.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                                .set("spark.akka.frameSize","100")
                                .set("spark.shuffle.consolidateFiles","true")
                                .set("spark.executor.uri","hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000/user/spark/spark-1.6.3-bin-hadoop2.6.tgz")
                                .set("spark.scheduler.mode", "FAIR")
                                .set("spark.mesos.coarse", "true")
				.set("spark.driver.maxResultSize", "10g")

val sc = new SparkContext(sparkConf)

// scala sql
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

// define the dto class schema
case class EVERYCLICK ( nowts:String, pcid:String, fsid:String, url:String, etc:String )
case class CART( pcid:String, sessionid:String, ts:String, cartitemsiteno:String, itemid:String, uitemid:String, ordqty:String, mastercartid:String, cartid:String, referer:String, ordItemInfloTgtId:String, dummy:String )

var c = Calendar.getInstance()
val year = new SimpleDateFormat("yy")
val month = new SimpleDateFormat("MM")
val dayStr = new SimpleDateFormat("dd")
val hour = new SimpleDateFormat("HH")

var date, date_1, everyclick_path, outputFilePath :String = null
var everyclickInfo :org.apache.spark.rdd.RDD[String] = null
var cartInfo :org.apache.spark.rdd.RDD[String] = null

var everyclickInfoArray:org.apache.spark.sql.DataFrame = null
var cartInfoArray:org.apache.spark.sql.DataFrame = null

val hadoop_url = "hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000"

   def init(args:Array[String]) {

        if (args.size > 0 && args(0) != "" && args(0).contains("-")) {
                var dt = args(0).split("-")
                if (dt.size > 3) {
                        var tempDate = new Date(100+dt(0).toInt,dt(1).toInt-1,dt(2).toInt,dt(3).toInt,0,0)
                        c.setTime(tempDate)
			c.add(Calendar.HOUR_OF_DAY,1)
                }
        }

	for (i <- 1 to 6) {
		c.add(Calendar.HOUR_OF_DAY,-1)
		var today = c.getTime()
		date = "%s-%s-%s/%s".format(year.format(today),month.format(today),dayStr.format(today),hour.format(today))
		println(date)
		if (i == 1) {
	                date_1 = date
	                //everyclick_path = hadoop_url+"/moneymall/tsv/every-click-event/"+date+"/*"
        		everyclickInfo = sc.textFile(hadoop_url+"/moneymall/tsv/every-click-event/"+date+"/*")
	        } else {
	                //everyclick_path = everyclick_path +","+hadoop_url+"/moneymall/tsv/every-click-event/"+date+"/*"
        		everyclickInfo ++= sc.textFile(hadoop_url+"/moneymall/tsv/every-click-event/"+date+"/*")
	        }
        }

        outputFilePath = hadoop_url+"/moneymall/tsv/previous-sales/"+date_1+"/"
        cartInfo = sc.textFile(hadoop_url+"/moneymall/tsv/cart-event-row/"+date_1+"/*")
        //everyclickInfo = sc.textFile(everyclick_path)
    
	// second dto class load
	everyclickInfoArray=everyclickInfo.map(_.split("\t"))
	.filter(row => row.length >= 65)
	.filter(row => !row(4).contains("/item/appItemLayer.ssg") &&
		       !row(4).contains("/item/appItemPrcDtlView.ssg") &&
		       !row(4).contains("/item/appItemInfoPopup.ssg") )
	.map(row => EVERYCLICK(
	        row(0),row(28),row(29),
		{
                        var tmpStr = row(4).toString
                        if (row(4)=="" && row(17)!="") tmpStr = row(17)
                        tmpStr
                },//url
        	row(1) +"\t"+row(2) +"\t"+row(3) +"\t"+row(4) +"\t"+
	        row(5) +"\t"+row(6) +"\t"+row(7) +"\t"+row(8) +"\t"+row(9)+"\t"+
       	 	row(10)+"\t"+row(11)+"\t"+row(12)+"\t"+row(13)+"\t"+row(14)+"\t"+
	        row(15)+"\t"+row(16)+"\t"+row(17)+"\t"+row(18)+"\t"+row(19)+"\t"+
	        row(20)+"\t"+row(21)+"\t"+row(22)+"\t"+row(23)+"\t"+row(24)+"\t"+
	        row(25)+"\t"+row(26)+"\t"+row(27)+"\t"+row(28)+"\t"+row(29)+"\t"+
	        row(30)+"\t"+row(31)+"\t"+row(32)+"\t"+row(33)+"\t"+row(34)+"\t"+
	        row(35)+"\t"+row(36)+"\t"+row(37)+"\t"+row(38)+"\t"+row(39)+"\t"+
	        row(40)+"\t"+row(41)+"\t"+row(42)+"\t"+row(43)+"\t"+row(44)+"\t"+
	        row(45)+"\t"+row(46)+"\t"+row(47)+"\t"+row(48)+"\t"+row(49)+"\t"+
	        row(50)+"\t"+row(51)+"\t"+row(52)+"\t"+row(53)+"\t"+row(54)+"\t"+
	        row(55)+"\t"+row(56)+"\t"+row(57)+"\t"+row(58)+"\t"+row(59)+"\t"+
	        row(60)+"\t"+row(61)+"\t"+row(62)+"\t"+row(63)+"\t"+row(64)+"\t"+"d"	//dummy 추가
	        )
	).toDF()

	cartInfoArray=cartInfo.filter(line => line.contains("\t")).map(_.split("\t")).map(row => CART(
		row(0),
		row(1),
		row(2),
		row(6),
		row(7),
		row(5),
		row(8),
		row(4),
		row(3),
		row(9),
               	if (row(17).contains("END")) "" else row(17),                    //새로 cartrow에 추가된 ordItemInfloTgtId
		"END"
	)).toDF()     

    }


var resultCount = 0
var indexCount = 0
var totalCount = 0
var keyHashMap = collection.mutable.Map[String,collection.immutable.TreeMap[String,String]]()
var sortHashMap = collection.mutable.Map[String,collection.immutable.TreeMap[String,String]]()

/////////////////////// MAIN 함수 파트 /////////////////////////////////
   def main(args:Array[String]) {
	val t0 = System.nanoTime()
	init(args)
	//println("Start : %s-%s-%s %s".format(year.format(today),month.format(today),dayStr.format(today),hour.format(today)))

     	inline(everyclickInfoArray ,cartInfoArray,outputFilePath)
	var t1 = System.nanoTime()
	println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
	println ("============================================================================")
   }

/////////////////////// SUB 함수 파트 //////////////////////////////////

   def inline(everyclickInfoArray:org.apache.spark.sql.DataFrame,  cartInfoArray:org.apache.spark.sql.DataFrame,  outputFilePath:String) {
    	everyclickInfoArray.registerTempTable("everyClick")
     	cartInfoArray.registerTempTable("cart")
     	var query = """
		SELECT
        		A.nowts, A.pcid, A.fsid, A.url, A.etc,	
                    	B.ts ,B.cartitemsiteno, B.itemid, B.uitemid, B.ordqty, B.mastercartid, B.cartid,
			B.referer, B.ordItemInfloTgtId
               FROM 
		   	(SELECT 
                                MAX(C.ts) AS ts, C.cartitemsiteno, C.itemid, C.uitemid, SUM(C.ordqty) AS ordqty, C.mastercartid, C.cartid, C.referer, C.pcid, C.sessionid, C.ordItemInfloTgtId
                         FROM   cart C
                         GROUP  BY C.cartitemsiteno, C.itemid, C.uitemid, C.mastercartid, C.cartid, C.referer, C.pcid, C.sessionid, C.ordItemInfloTgtId ) AS B
	       LEFT OUTER JOIN everyClick AS A 
               ON 
                    A.pcid = B.pcid
	       AND  A.fsid = B.sessionid
	       WHERE B.ts >= A.nowts
     	"""

     	val result = sqlContext.sql(query)
	println("query result");	
	//test용
	//result.collect().take(600000).foreach(sort(_))
	result.collect().foreach(sort(_))
	println("sort end")
	println("[LastResult][RESULT_COUNT/TOTAL_COUNT]"+resultCount+","+totalCount)
	//println("[Last Result]"+keyHashMap);
	var resultArray:Array[String] = new Array[String](resultCount)
	keyHashMap.foreach(p => arrangeData( p._1 , p._2));
	println("data arrange end")
	//println(keyHashMap)
	keyHashMap.foreach(p => lastResult( p._1 , p._2 , resultArray));
	println("keyHashMap.size = " + keyHashMap.size)
	sc.makeRDD(resultArray).coalesce(1).saveAsTextFile(outputFilePath)	

    }

    // syncronized map
    def sort(result:org.apache.spark.sql.Row) {
	var key = "%s\t%s\t%s\t%s\t%s\t%s\t%s".format(result(6),result(7),result(8),result(9),result(10),result(11),result(5))
	var keyOfTreeMap = "%s".format(result(0))
	var valuesOfTreeMap = result(4).toString.split("\t")
        valuesOfTreeMap(62) = result(12).toString//cart_referer
	valuesOfTreeMap(46) = result(13).toString//ordItemInfloTgtId

        if ( keyHashMap.contains(key) ) {
                var valueTreeMap = keyHashMap(key)
                valueTreeMap += (keyOfTreeMap -> valuesOfTreeMap.mkString("\t"))
                keyHashMap += (key -> valueTreeMap)

        }
        else {
                resultCount += 1
                var valueTreeMap = collection.immutable.TreeMap[String,String]()
                valueTreeMap += (keyOfTreeMap -> valuesOfTreeMap.mkString("\t"))
                keyHashMap += (key -> valueTreeMap)
        }
        totalCount += 1
        //if ((totalCount % 1000) == 0) println (totalCount)
    }
    // Data arrange
    def arrangeData(key:String,value:collection.immutable.TreeMap[String,String]) {
        //key를 역순으로 정열
        var tempvalue = new collection.immutable.TreeMap[String,String]()
        var keysSeq = value.keySet.toIndexedSeq.reverse
        //println("aa:"+keysSeq+"/"+keysSeq.size)

        //사용변수 초기화
        var before_tm:String=""
        var row_url:String=""
        var depth1:String=""
        var tm:String=""
        var delete_gbn:String=""
        var delete_check_value:String=""
        var tarea:String=""
        var tarea_flag:Boolean=false
	var key_itemid=key.split("\t")(1)

        //reload, 그냥 막 지울페이지 정리
        for (i <- keysSeq) {
                row_url = value(i).split("\t")(3)
		depth1 = value(i).split("\t")(16)
                //63이 end      
                //막 지우는 페이지 플래그 처리
                if ( row_url.contains("item/itemDetailRate.ssg") ||
                     row_url.contains("item/itemDetailDesc.ssg") ||
                     row_url.contains("item/itemDetailQna.ssg")  ||
                     row_url.contains("item/itemDetailReturn.ssg")  ||
                     row_url.contains("item/itemDetailDescOrgView.ssg")  ||
                     row_url.contains("item/itemComment.ssg")    ||
                     row_url.contains("item/itemSimple.ssg")     ||
                     row_url.contains("item/itemViewSub.ssg")    ||
                     row_url.contains("item/itemQna.ssg")    ||
                     row_url.contains("item/itemQnaPageList.ssg")    ||
                     row_url.contains("item/itemImage.ssg")    ||
                     row_url.contains("item/itemCard.ssg")    ||
                     row_url.contains("item/appItemView.ssg")    ||
                     row_url.contains("item/appItemDescInfoView.ssg")    ||
		     row_url.contains("item/appItemLayer.ssg")    ||
                     row_url.contains("item/appItemPrcDtlView.ssg")    ||
                     row_url.contains("item/popup/popupCoupon.ssg")  ||
                     row_url.contains("item/popup/popupItemChnl.ssg")||
                     row_url.contains("item/popup/couponInfo.ssg")   ||
                     row_url.contains("item/popup/popupProductImg.ssg")   ||
                     row_url.contains("item/popup/popupMoreView.ssg")   ||
                     row_url.contains("item/popup/popupReserve.ssg")   ||
                     row_url.contains("/shipping/showPopupDelivery.ssg")||
                     row_url.contains("/event/couponInfo.ssg")||
                     row_url.contains("/event/firstbuyCpn.ssg")||
                     row_url.contains("/obanjang/popupObanjang.ssg")||
                     row_url.contains("/channel/popupHappyBuyrus.ssg")||
                     row_url.contains("/cart/dmsShpp.ssg")||
		     row_url.contains("/cart/perdcShpp.ssg") ||
		     row_url.contains("/cart/getRplcItemList.ssg") ||
                     row_url.contains("/order/ordPage.ssg")||
                     row_url.contains("/order/ocbCheckPop.ssg")||
                     row_url.contains("/order/popup.ssg")||
                     row_url.contains("/order/spointCheckPop.ssg")||
                     row_url.contains("/order/loadPaymtAuthPage.ssg")||
                     row_url.contains("/myssg/popup/popupMbrGrdCpn.ssg")||
                     row_url.contains("/myssg/moneyMng/memberCpnOwnList.ssg")||
                     row_url.contains("/myssg/cancelProcess.ssg")||
                     row_url.contains("/comm/app")||
                     row_url.contains("/comm/evnt/promBanr.ssg")||
                     row_url.contains("/comm/chnlPopup.ssg")||
                     row_url.contains("/appApi")||
                     row_url.contains("/comm/popup")||
		     row_url.contains("/comm/zipcd/loadPaymtPage.ssg") ||
                     row_url.contains("/customer")||
		     row_url.contains("/nodcsnOrder/drctPurch.ssg") ||
		     row_url.contains("/nodcsnOrder/ordShppInfo.ssg") ||
		     row_url.contains("/item/pItemDtlDesc.ssg") ||
		     row_url.contains("/item/popup/popupItemView.ssg") ||
                     row_url.contains("member.") ||
	   	     depth1.contains("Login")) {

                        if (value(i).length > 0) delete_check_value = "1"
                        
                } else {
                        if (value(i).length > 0) delete_check_value = "0"

                }

                //tarea처리
		if (row_url.contains("&tarea=") && row_url.substring(row_url.indexOf("&tarea=")).length > 7) {
                        tarea = row_url.split("&tarea=")(1).split("&")(0)
                        tarea_flag=true
                } else if (row_url.contains("&click=") && row_url.substring(row_url.indexOf("&click=")).length > 7) {
                        tarea = row_url.split("&click=")(1).split("&")(0)
                        tarea_flag=true
                } else if (row_url.contains("&CatForYou=") && row_url.substring(row_url.indexOf("&CatForYou=")).length > 11) {
                        tarea = "CatForYou_"+row_url.split("&CatForYou=")(1).split("&")(0)
                        tarea_flag=true
                } else if (row_url.contains("&ForYouB=") && row_url.substring(row_url.indexOf("&ForYouB=")).length > 9) {
                        tarea = "ForYouB_"+row_url.split("&ForYouB=")(1).split("&")(0)
                        tarea_flag=true
                } else {
                        if (tarea_flag) {
                                tarea_flag=false        //추가했으니까 false로 다시 바꿈
                        } else {
                                tarea = ""
                        }
                }

                //flag add
                var t = value(i).split("\t")
                //t(60) = delete_check_value
                t(64) = delete_check_value
                t(61) = tarea
                t(44) = ""	//cache여부 삭제 (reload제거할라고)
                t(45) = ""	//duratinTime 삭제(마찬가지)
                tm = t.mkString("\t")

                //리로드페이지 제거 (PC & MW)
		if (tm != before_tm) {
                        tempvalue += (i -> tm)
                }

                before_tm = tm
        }

        //keyHashMap += (key -> tempvalue)
	var tempkeysSeq = tempvalue.keySet.toIndexedSeq.reverse
        //println("temp:"+tempkeysSeq+"/"+tempkeysSeq.size)
        var first_delete_flag:Boolean = true
        var first_temp_string:String = ""
        //var cv = tempvalue
        //println("keyset = " + tempkeysSeq)
        for (t <- tempkeysSeq) {
                //reload 정리
		//if (!tempvalue(t).split("\t")(16).equals("")) row_url = tempvalue(t).split("\t")(16)
                row_url = tempvalue(t).split("\t")(3)
                depth1 = tempvalue(t).split("\t")(16)
                
		//println("row_url = " + first_delete_flag +"/"+row_url)

                delete_gbn = tempvalue(t).split("\t")(64)
                //if (delete_gbn !="0" && delete_gbn !="1") {
                //println("delete_gbn = " + delete_gbn)
                //}
                //삭제해야 할 것들은 볼 필요도 없이 삭제해버림
                if (delete_gbn == "1") {
                        if (tempkeysSeq.size != 1) {
                                tempvalue -= t
                        }
                } else {
                        //첫페이지인지 체크
                        if (first_delete_flag) {
                                if (row_url.contains("/itemView.ssg")      ||
                                    row_url.contains("/itemView01.ssg")    ||
                                    row_url.contains("/itemDtl.ssg") ||
                                    row_url.contains("/itemDetailRate.ssg") ||
                                    row_url.contains("/itemDetailDesc.ssg") ||
				    row_url.contains("/dealItemView.ssg") ||
                                    row_url.contains("/item/itemSimple.ssg") ||
                                    row_url.contains("/quickItemView.ssg") ||
                                    depth1.contains("ItemDetail") ||	//mobileApp 구버전 네이티브상품상세
                                    depth1.contains("ProductDealNativeFragment") ||	//mobileApp 안드로이드 신버전 딜상품상세
                                    depth1.contains("DealViewController") ||	//mobileApp iOS 신버전 딜상품상세
                                    depth1.contains("ProductDetailNativeFragment") ||	//mobileApp 안드로이드 신버전 상품상세
                                    depth1.contains("ItemDetailViewController") 	//mobileApp iOS 신버전 상품상세
				) {
					//println(t + "/" + key_itemid + "/" + row_url )
                                 	if (tempkeysSeq.size != 1) {
                                        	first_temp_string = t +"|"+ tempvalue(t).toString
                                                tempvalue -= t
						//println(" -> 삭제됨" + t)
					}
                                }
                        }
                        first_delete_flag = false
                }
		//println("")
        }

        if (tempvalue.size == 0 && first_temp_string != "") {
                tempvalue =  collection.immutable.TreeMap(first_temp_string.split("|")(0) -> first_temp_string.split("|")(1))

        }

        if (tempvalue.size > 0) {
                keyHashMap += (key -> tempvalue)
        } else {
                println("다 지워서 넣을것이 없습니다!")
        }

    }

    // Last Result for foreach merge 
    def lastResult(key:String,value:collection.immutable.TreeMap[String,String],resultArray:Array[String]) {
	resultArray(indexCount) = "%s\t%s\t%s".format(key,value.lastKey,value(value.lastKey))
	indexCount += 1		
    }	
}
