// Donghoon Seo
// 2017-02-01
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json._
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.get_json_object

object AdvertiseRow {

	// sc initialize
	val sparkConf = new SparkConf().setAppName("AdvertiseRow")
                                .set("spark.hadoop.validateOutputSpecs", "false")
                                .set("spark.rpc.message.maxSize","100")
                                .set("spark.local.dir","/data01/spark_tmp/")
                                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                                .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
                                .set("spark.executor.uri","hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000/user/spark/spark-1.6.3-bin-hadoop2.6.tgz")
                                .set("spark.scheduler.mode", "FAIR")
                                //.set("spark.sql.tungsten.enabled", "false")
                                .set("spark.mesos.coarse","true")

	val sc = new SparkContext(sparkConf)
        sc.setLogLevel("INFO")
	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._
	import org.apache.spark.sql.functions._

	// 기준일
	val yyyy = new SimpleDateFormat("yyyy")
	val yy = new SimpleDateFormat("yy")
	val mm = new SimpleDateFormat("MM")
	val dd = new SimpleDateFormat("dd")
	val hh = new SimpleDateFormat("HH")

	var date:String=""
	var hour:String=""

	var adRDD:org.apache.spark.rdd.RDD[String]=null

	var outputFilePath:String=""
	var outputFilePath2:String=""

	var adDF:org.apache.spark.sql.DataFrame=null

	val c = Calendar.getInstance()
	c.add(Calendar.HOUR_OF_DAY,-1);

	def init(args:Array[String]) {

        	if (args.size > 0 && args(0) != "" && args(0).contains("-")) {
                	var dt = args(0).split("-")
	                if (dt.size > 2) {
        	                var tempDate = new Date(100+dt(0).toInt,dt(1).toInt-1,dt(2).toInt,dt(3).toInt,0,0)
                	        c.setTime(tempDate)
	                }
	        }
	        val today = c.getTime()
		date = "%s-%s-%s".format(yy.format(today),mm.format(today),dd.format(today))
		hour = "%s".format(hh.format(today))

	        println("Start : %s / %s".format(date, hour))

		adRDD = sc.textFile("hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000/moneymall/tsv/advertise-event/"+date+"/"+hour+"/*")
		outputFilePath = "hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000/moneymall/tsv/advertise-row/"+date+"/"+hour

		// udf function 
		val toJsonArrayStringArr = udf((jsonArrStr:String) => jsonArrStr match { 
			case null => None
			case s => Some(s.replace("[","").replace("]","").replace("},{" , "}},{{").split("\\},\\{"))
		})

		// dto class load
		adDF=adRDD.filter(line => line.contains("\t")).map(_.split("\t"))
			.filter(c => c.length >= 65)
			.filter(c => c(44) != "")
			.map(c => (
        		c(44).toLowerCase,	//advertise json data
			c(0),	//ts 1
			c(28),//pcid 2
			c(34),//mbrid 3 
			c(29),	//fsid 4
			c(18),	//referrer 5
			c(19)	//userAgent FullString 6
		))
		.toDF("jsonData","ts","pcid","mbrid","fsid","referrer","ua")
		.select(
			$"*",
			get_json_object($"jsonData", "$.position") as "position",	//7
			get_json_object($"jsonData", "$.userip") as "userip",	//8
			get_json_object($"jsonData", "$.globalid") as "globalid",	//9
			get_json_object($"jsonData", "$.mediacd") as "mediacd",	//10
			get_json_object($"jsonData", "$.areaid") as "areaid",	//11
			get_json_object($"jsonData", "$.type") as "type",	//12
			get_json_object($"jsonData", "$.siteno") as "siteno",	//13
			get_json_object($"jsonData", "$.unitinfolist") as "list" )
		.withColumn("unitinfolist", explode(toJsonArrayStringArr($"list")))
		.select( 
			$"*",
			get_json_object($"unitinfolist", "$.adtgtid") as "adtgtid",	//16
			get_json_object($"unitinfolist", "$.adidx") as "adidx",	//17
			get_json_object($"unitinfolist", "$.advertbilngtypecd") as "advertbilngtypecd",	//18
			get_json_object($"unitinfolist", "$.advertacctid") as "advertacctid",	//19
			get_json_object($"unitinfolist", "$.advertbidid") as "advertbidid",	//20
			get_json_object($"unitinfolist", "$.advertkindcd") as "advertkindcd",	//21
			get_json_object($"unitinfolist", "$.unittype") as "unittype",	//22
			get_json_object($"unitinfolist", "$.advertextensterydivcd") as "advertextensterydivcd"	//23
        	)
		.withColumn("advertextensterydivcd", when($"advertextensterydivcd".isNull or $"advertextensterydivcd" === "","10").otherwise($"advertextensterydivcd"))

		adDF.map(c => {"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"
			.format(
				c(1),//ts
				c(2),//pcid
				c(3),//mbrid
				c(4),//fsid
				c(5),//referrer
				c(6),//ua
				c(12),	//type
				c(9),	//globalid
				c(11),	//areaid
				c(13),	//siteno
				c(10),	//mediacd
				c(7),	//position
				c(22),	//unittype
				c(19),	//advertacctid
				c(20),	//advertbidid
				c(16),	//adtgtid
				c(17),	//adidx
				c(23),	//advertExtensTeryDivCd 180208 ADD
				"END"
			).replace("null","")
	        })
	        .coalesce(1).saveAsTextFile(outputFilePath)

    	}

    	/////////////////////// MAIN 함수 파트 /////////////////////////////////
    	def main(args:Array[String]) {
       		val t0 = System.nanoTime()

		init(args)

        	println ("============================================================================")
		var t1 = System.nanoTime()
        	println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        	println ("============================================================================")
    	}

}
