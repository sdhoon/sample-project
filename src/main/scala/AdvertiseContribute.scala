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

object AdvertiseContribute {

// sc initialize
val sparkConf = new SparkConf().setAppName("AdvertiseContribute")
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
                if (dt.size > 3) {
                        var tempDate = new Date(100+dt(0).toInt,dt(1).toInt-1,dt(2).toInt,dt(3).toInt,0,0)
                        c.setTime(tempDate)
                }
        }
        val today = c.getTime()
	date = "%s-%s-%s".format(yy.format(today),mm.format(today),dd.format(today))
	hour = hh.format(today)

        println("Start : %s / %s".format(date, hour))

	adRDD = sc.textFile("hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000/moneymall/tsv/order-master/"+date+"/"+hour+"/*")
	outputFilePath = "hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000/moneymall/tsv/advertise-contribute/"+date+"/"+hour

	// dto class load
	adDF=adRDD.filter(line => line.contains("\t")).map(_.split("\t"))
		.filter(c => c(67) != "" && (c(67).length == 10 || c(67).length == 12))
		.map(c => (
			{
				if (c(67).length == 12) c(67).substring(2)
				else c(67)
			},	//ordItemInfloTgtId
			c(0),	//ordNo
			new java.text.SimpleDateFormat("MMddHHmm").format(new java.sql.Timestamp(c(4).toLong).getTime()),	//ts
                        {
                                if (c(67).length == 12) c(67).substring(0,2)
                                else "10"
                        },      //ADVERT_EXTENS_TERY_DIV_CD
			{
				if (c(40).equals("10")) "10"
				else {
					if (c(39).contains("SSGAPP")) "30"
					else "20"
				}
			},	//dvicDivCd
			c(18),	//cartid
			new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.sql.Timestamp(c(19).toLong).getTime()),	//cart_timestamp
			(c(2).toInt * c(3).toInt)	//ord_sales_amt
			)	
		).toDF()

	adDF.map(c => {"%s,%s,%s,%s,%s,%s,%s,%s".format(c(0),c(1),c(2),c(3),c(4),c(5),c(6),c(7)) })
	.coalesce(1)
        .saveAsTextFile(outputFilePath)


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
