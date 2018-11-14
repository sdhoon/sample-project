// Donghoon Seo
// Spark 1.5.2
// 2016-01-14
// 2016-09-09
// 방문원주문건수로 수정되었음 일반 orderEventRow는 OrderMappingOriginal 로 확인하면 됩니다.
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json._
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat

object OrderMapping {

// define the dto class schema
case class ORDER( itemlist:String, orderData:String, addData:String )

// sc initialize
val sparkConf = new SparkConf().setAppName("OrderMapping")
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

var orderInfo:org.apache.spark.rdd.RDD[String] = null
var orderInfoArray:org.apache.spark.sql.DataFrame = null
var outputFilePath:String=""
var outputFilePath_original:String=""

val hadoop_url = "hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000"

val year = new SimpleDateFormat("yy")
var yearfull = new SimpleDateFormat("yyyy")
val month = new SimpleDateFormat("MM")
val dayStr = new SimpleDateFormat("dd")
val hour = new SimpleDateFormat("HH")

var c = Calendar.getInstance()
c.add(Calendar.HOUR_OF_DAY,-1)

   def init(args:Array[String]) {

        if (args.size > 0 && args(0) != "" && args(0).contains("-")) {
                var dt = args(0).split("-")
                if (dt.size > 3) {
                        var tempDate = new Date(100+dt(0).toInt,dt(1).toInt-1,dt(2).toInt,dt(3).toInt,0,0)
                        c.setTime(tempDate)
                }
        }
        val today = c.getTime()
	var date = "%s-%s-%s/%s".format(year.format(today),month.format(today),dayStr.format(today),hour.format(today))

        println("date = " + date + "/" + yearfull.format(today))
    
	// hadoop file load
	orderInfo = sc.textFile(hadoop_url + "/moneymall/tsv/order-event/"+date+"/*")
	outputFilePath = hadoop_url + "/moneymall/tsv/order-event-row/"+date
	outputFilePath_original = hadoop_url + "/moneymall/tsv/order-event-row-original/"+date

	// second dto class load
	orderInfoArray=orderInfo.map(_.split("\t"))
		//20160409 Ryu Sung: out of index 에러 방어코드
		.filter(row => row.length >= 61)
		.map(row => ORDER(
		row(44),
		row(20)+"\t"+row(21)+"\t"+row(22)+"\t"+row(23)+"\t"+row(24)+"\t"+row(25)+"\t"+row(26)+"\t"+
		row(58)+"\t"+row(59)+"\t"+row(29),
		row(6)+"\t"+row(55)
		)
	).toDF()     
}
/////////////////////// MAIN 함수 파트 /////////////////////////////////
   def main(args:Array[String]) {
	val t0 = System.nanoTime()
	init(args)
	//println("date " + date + " " + hour.format(today))
     	inline(orderInfoArray,outputFilePath)
	var t1 = System.nanoTime()
	println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
	println ("============================================================================")
   }

/////////////////////// SUB 함수 파트 //////////////////////////////////

class ClassCaster[T] { def unapply(a:Any):Option[T] = Some(a.asInstanceOf[T]) }
object AsMap extends ClassCaster[Map[String, Any]]
object AsList extends ClassCaster[List[Any]]
object AsString extends ClassCaster[String]
object AsDouble extends ClassCaster[Double]

   def inline( orderInfoArray:org.apache.spark.sql.DataFrame,  outputFilePath:String) {

	var resultArray = ArrayBuffer[String]()
	var resultArray_original = ArrayBuffer[String]()

	//println("orderInfoArray's count == " + orderInfoArray.count)
	orderInfoArray.collect().foreach(a => {
		//println(JSON.parseFull(a.itemlist)) 
		var jsonlist = for {
		//println(JSON.parseFull(a.itemList))
			Some(AsMap(map)) <- List(JSON.parseFull(a(0).toString))
			AsMap(bm) = map("baseProperties")
			AsString(ckWhere) = bm("ckWhere")
			AsString(mbrid) = bm("mbrId")
			AsString(pcid) = bm("pcid")
			AsString(dmid) = bm("dmId")
			AsString(sessionId) = bm("sessionId")
			AsString(ip) = bm("ip")
			AsString(browser) = bm("browser")
			AsString(tgtMediaCd) = bm("tgtMediaCd")
			AsDouble(timestamp) = bm("timestamp")
			AsList(list) = map("subOrderItemList")
			AsString(ordNo) = map("ordNo")
			AsString(ordCmplDts) = map("ordCmplDts")
			AsMap(m) <- list
			AsDouble(ordItemSeq) = m("ordItemSeq")
			AsDouble(unitPrice) = m("unitPrice")
			AsString(cartItemSiteNo) = m("cartItemSiteNo")
			AsString(masterCartId) = m("masterCartId")
			AsString(cartId) = m("cartId")
			AsString(uitemId) = m("uitemId")
			AsDouble(ordQty) = m("ordQty")
			AsString(itemId) = m("itemId")
			AsString(infloSiteNo) = m("infloSiteNo")
			AsString(ordItemRepTypeCd) = m("ordItemRepTypeCd")
			AsString(ordItemTypeCd) = m("ordItemTypeCd")
			AsString(itemChrctDtlCd) = m("itemChrctDtlCd")
		} yield "%s|%d|%s|%s|%s|%s|%s|%d|%d|%s|%.0f|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s"
		.format(ordNo,ordItemSeq.toInt,cartItemSiteNo,masterCartId,cartId,itemId,uitemId,unitPrice.toInt,ordQty.toInt,ckWhere,timestamp,tgtMediaCd,browser,ip,sessionId,dmid,pcid,mbrid,ordCmplDts,infloSiteNo,ordItemRepTypeCd,ordItemTypeCd,itemChrctDtlCd)
		if (jsonlist.size > 0) {
			for (m <- jsonlist) {
				var mm = m.split('|')
				//original도 그냥 저장
				resultArray_original += "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s".format(mm(0),mm(1),mm(2),mm(3),mm(4),mm(5),mm(6),mm(7),mm(8),mm(9),mm(10),mm(11),mm(12),mm(13),mm(14),mm(15),mm(16),mm(17),a(1).toString,mm(18),mm(19),mm(20),mm(21),mm(22),a(2).toString,"END")

				if ((!mm(20).equals("30") || mm(21).equals("12")) &&
				     !mm(22).startsWith("6") && !mm(22).startsWith("8") && !mm(22).startsWith("9")) {	//방문원주문건수로 맞추기 위한 filtering 조건 추가 160909
					resultArray += "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s".format(mm(0),mm(1),mm(2),mm(3),mm(4),mm(5),mm(6),mm(7),mm(8),mm(9),mm(10),mm(11),mm(12),mm(13),mm(14),mm(15),mm(16),mm(17),a(1).toString,mm(18),mm(19),mm(20),mm(21),mm(22),a(2).toString,"END")
				}
			}
		} else {
			println("없다!")
		}
	})
	sc.makeRDD(resultArray).coalesce(1).saveAsTextFile(outputFilePath)
	sc.makeRDD(resultArray_original).coalesce(1).saveAsTextFile(outputFilePath_original)

    }

}
