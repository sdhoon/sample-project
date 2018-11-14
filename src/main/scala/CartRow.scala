// Donghoon Seo
// Spark 1.6 버전으로 바꿈
// 2016-01-14

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json._
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat

object CartRow {

// define the dto class schema
case class CART( pcid:String, fsid:String, ts:String, cartjson:String, referer:String, mbrid:String, deviceType:String, browser:String, ua:String, ip:String, dmid:String, ckwhere:String, domainSiteNo:String, mobileDomain:String )

// sc initialize
val sparkConf = new SparkConf().setAppName("CartRow")
                                .set("spark.hadoop.validateOutputSpecs", "false")
                                .set("spark.akka.frameSize","100")
                                .set("spark.local.dir","/data01/spark_tmp/")
                                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                                .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
                                .set("spark.executor.uri","hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000/user/spark/spark-1.6.3-bin-hadoop2.6.tgz")
                                .set("spark.scheduler.mode", "FAIR")
                                .set("spark.sql.tungsten.enabled", "false")
				.set("spark.mesos.coarse", "true")

val sc = new SparkContext(sparkConf)

// scala sql
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

var cartInfo:org.apache.spark.rdd.RDD[String] = null
var cartInfoArray:org.apache.spark.sql.DataFrame = null
var outputFilePath:String=""

val hadoop_url = "hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000"

val year = new SimpleDateFormat("yy")
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
        val date = "%s-%s-%s".format(year.format(today),month.format(today),dayStr.format(today))
	val hh = hour.format(today)

        println("date = " + date + "/" + hh)

	// hadoop file load
	cartInfo = sc.textFile(hadoop_url + "/moneymall/tsv/cart-event/"+date+"/"+hh+"/*")

	outputFilePath = hadoop_url + "/moneymall/tsv/cart-event-row/"+date+"/"+hh+"/"

	// second dto class load
	cartInfoArray=cartInfo.filter(line => line.contains("\t")).map(_.split("\t")).filter(row => row.length > 45).map(row => CART(
        	row(28),
	        row(29),
        	row(0),
	        row(44),
        	if(row(18)!="" ) row(18) else ".",
		row(34), 	//mbrid
		row(20),	//OS-deviceType
		row(22),	//OS-browser
		row(19),	//userAgent_FullString
		row(2),		//ip
		row(33),	//dmid
		row(30),	//ckwhere
		row(6),		//domainSiteNo
		row(55)		//mobileDomain
	        )
	).toDF()

    }

/////////////////////// MAIN 함수 파트 /////////////////////////////////
   def main(args:Array[String]) {
	val t0 = System.nanoTime()
	init(args)
	//println("date " + date + " " + hour.format(today))
     	inline(cartInfoArray,outputFilePath)
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

   def inline( cartInfoArray:org.apache.spark.sql.DataFrame, outputFilePath:String) {
	try {
		var resultArray = ArrayBuffer[String]()
		//println("cartInfoArray's count == " + cartInfoArray.count)
		cartInfoArray.collect().foreach(a => {
			//println(a(0)+"/"+a(1)+"/"+a(2)+"/"+a(3))
			//pcid:String, fsid:String, ts:String, cartjson:String, referer:String
		
			var jsonlist = for {
				Some(AsList(list)) <- List(JSON.parseFull(a(3).toString))
				AsMap(m) <- list
				AsString(cartId) = m("cartId")
				AsString(masterCartId) = m("masterCartId")
				AsString(uitemId) = m("uitemId")
				AsString(cartItemSiteNo) = m("cartItemSiteNo")
				AsString(itemId) = m("itemId")
				AsDouble(ordQty) = m("ordQty")
				AsList(adlist) = if (m.exists(_._1 == "ordItemInfloExtApiDtoList") && m("ordItemInfloExtApiDtoList") != null) m("ordItemInfloExtApiDtoList") else List(Map("ordItemInfloTgtId" -> "", "ordItemInfloDivCd" -> "0"))
                                AsMap(adm) <- adlist
                                AsString(ordItemInfloTgtId)  = if (adm.exists(_._1 == "ordItemInfloTgtId")) adm("ordItemInfloTgtId") else ""
                                AsString(ordItemInfloDivCd)  = if (adm.exists(_._1 == "ordItemInfloDivCd")) adm("ordItemInfloDivCd") else ""
			} yield "%s\t%s\t%s\t%s\t%s\t%d|%s|%s|%s".format(cartId, masterCartId, uitemId, cartItemSiteNo, itemId, ordQty.toInt, ordItemInfloTgtId, ordItemInfloDivCd, "10")

			if (jsonlist.size > 0) {
				for (m <- jsonlist) {
					var mm = m.split('|')
					if (mm(2)!="03") resultArray += "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s".format(a(0), a(1), a(2), mm(0), a(4), a(5), a(6), a(7),a(8),a(9),a(10),a(11),mm(1),a(12),a(13),"END")
				}
			}

		})
		sc.makeRDD(resultArray).coalesce(1).saveAsTextFile(outputFilePath)
	} catch {
		case e: Exception => println(e.toString)
	}

    }

}
