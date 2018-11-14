// Donghoon Seo
// 2017-02-01
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json._

object AdvertiseAggregate {

// sc initialize
val sparkConf = new SparkConf().setAppName("AdvertiseAggregate")
                                .set("spark.hadoop.validateOutputSpecs", "false")
                                .set("spark.rpc.message.maxSize","100")
                                .set("spark.local.dir","/data01/spark_tmp/")
                                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                                .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
                                .set("spark.executor.uri","hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000/user/spark/spark-1.6.3-bin-hadoop2.6.tgz")
                                .set("spark.scheduler.mode", "FAIR")
                                .set("spark.mesos.coarse","true")

val sc = new SparkContext(sparkConf)

// scala sql
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

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
	hour = hh.format(today)

        println("Start : %s / %s".format(date, hour))

	adRDD = sc.textFile("hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000/moneymall/tsv/advertise-row/"+date+"/"+hour+"/*")
	outputFilePath = "hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000/moneymall/tsv/advertise-aggregate/"+date+"/"+hour

	// dto class load
	adDF=adRDD.filter(line => line.contains("\t")).map(_.split("\t"))
		.filter(c => c.length >= 16)
		.map(c => (
		new java.text.SimpleDateFormat("MMddHHmm").format(new java.sql.Timestamp(c(0).toLong).getTime()),	//ts
		c(1),//pcid
		c(2),//mbrid 
		c(3),	//fsid
		c(4),	//referrer
		c(5),	//userAgent FullString
		c(6),	//type
		c(7).replace("해피바이러스","happy").replace("오반장","obanjang"),	//globalid
		c(8),	//areaid
		c(9),	//siteno
		c(10),	//mediacd
		c(11),	//position
		{	
			if (c(12) == "item") "10" 
			else if (c(12) == "banr" || c(12) == "main_banr" ) "20" 
			else "" 
		},	//unittype
		c(13),	//advertacctid
		c(14),	//advertbidid
		c(15),	//adtgtid
		c(16),	//adidx
		c(17)	//advertextensterydivcd
	)).toDF("ts","pcid","mbrid","fsid","referrer","ua","type","globalid","areaid","siteno","mediacd","position","unittype","advertacctid","advertbidid","adtgtid","adidx","advertextensterydivcd")

	//println ("========"+adDF.count.toString)

    }

    /////////////////////// MAIN 함수 파트 /////////////////////////////////
    def main(args:Array[String]) {
       	val t0 = System.nanoTime()

	init(args)

	aggregate(adDF)

        println ("============================================================================")
	var t1 = System.nanoTime()
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")
    }

    /////////////////////// SUB 함수 파트 //////////////////////////////////
    def aggregate(adDF:org.apache.spark.sql.DataFrame) {
	//advertacctid, advertbidid, ts, adverttgtdivcd, adverttgtid, click_cnt, pv_cnt, globalid, areaid, itemView_cnt, cart_cnt, directPurchage_cnt, clip_cnt, quickView_cnt, share_cnt, deviceType, siteno
	adDF.registerTempTable("ad")
        var query1 = """
		SELECT 
			 A.siteno, A.mediacd, A.ts, A.globalid, A.areaid, A.unittype, A.advertacctid, A.advertbidid, A.adtgtid, A.advertextensterydivcd
			,if (sum(pv_cnt) is null, 0, sum(pv_cnt))
			,if (sum(click_cnt) is null, 0, sum(click_cnt))
			,if (sum(view_cnt) is null, 0, sum(view_cnt))
			,if (sum(cart_cnt) is null, 0, sum(cart_cnt))
			,if (sum(buy_cnt) is null, 0, sum(buy_cnt))
			,if (sum(clip_cnt) is null, 0, sum(clip_cnt))
			,if (sum(quick_cnt) is null, 0, sum(quick_cnt))
			,if (sum(share_cnt) is null, 0, sum(share_cnt))
			,if (sum(pop_cnt) is null, 0, sum(pop_cnt))
			--,if (sum(tab_cart_cnt) is null, 0, sum(tab_cart_cnt))
			--,if (sum(tab_clip_cnt) is null, 0, sum(tab_clip_cnt))
			--,if (sum(tab_share_cnt) is null, 0, sum(tab_share_cnt))
		FROM (
                SELECT
                         siteno, mediacd, ts, globalid, areaid, unittype, advertacctid, advertbidid, adtgtid, advertextensterydivcd 
			,case when type = 'pv' then 1 end as pv_cnt
			,case when type = 'click' then 1 end as click_cnt
			,case when position = 'view' then 1 end as view_cnt
			,case when (position = 'cart' or position = 'tap_cart') then 1 end as cart_cnt
			,case when position = 'buy' then 1 end as buy_cnt
			,case when (position = 'clip' or position = 'tap_clip') then 1 end as clip_cnt
			,case when position = 'quick' then 1 end as quick_cnt
			,case when (position = 'share' or position = 'tap_share' ) then 1 end as share_cnt
			,case when position = 'pop' then 1 end as pop_cnt
			--,case when position = 'tap_cart' then 1 end as tap_cart_cnt
			--,case when position = 'tap_clip' then 1 end as tap_clip_cnt
			--,case when position = 'tap_share' then 1 end as tap_share_cnt
                FROM    ad ) A
		GROUP BY A.siteno, A.mediacd, A.ts, A.globalid, A.areaid, A.unittype, A.advertacctid, A.advertbidid, A.adtgtid, A.advertextensterydivcd
		ORDER BY A.siteno, A.mediacd, A.ts, A.globalid, A.areaid, A.unittype, A.advertacctid, A.advertbidid, A.adtgtid, A.advertextensterydivcd
        """
        var result1 = sqlContext.sql(query1)
	//println("-----------------------------------------------------------------------------------------------")
	//result1.collect().foreach(println)
	//println("-----------------------------------------------------------------------------------------------")
        //every_click_event_view

        result1.map ( c => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s".format(c(6),c(7),c(2),c(5),c(8),c(9),c(11),c(10),c(3),c(4),c(12),c(13),c(14),c(15),c(16),c(17),c(18),c(0),c(1)))
	.coalesce(1)
        .saveAsTextFile(outputFilePath)

    }

}
