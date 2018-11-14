// Donghoon Seo
// Spark 1.6 Version
// 2016-01-15

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util._
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer

object MakeSalesPageSet {

// sc initialize
val sparkConf = new SparkConf().setAppName("MakeSalesPageSet")
                                .set("spark.hadoop.validateOutputSpecs", "false")
                                .set("spark.local.dir","/data01/spark_tmp/")
                                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
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

var c = Calendar.getInstance()
val year = new SimpleDateFormat("yy")
val month = new SimpleDateFormat("MM")
val dayStr = new SimpleDateFormat("dd")
val hour = new SimpleDateFormat("HH")

var outputFilePath :String = null
var spcshopRDD :org.apache.spark.rdd.RDD[String] = null
var spcshopDF :org.apache.spark.sql.DataFrame = null

var pagesetRDD :org.apache.spark.rdd.RDD[String] = null
var pagesetDF :org.apache.spark.sql.DataFrame = null

val hadoop_url = "hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000"

    def init(args:Array[String]) {

	spcshopRDD = sc.textFile(hadoop_url+"/moneymall/tsv/sales-page-set/spcshop/*")
	pagesetRDD = sc.textFile(hadoop_url+"/moneymall/tsv/sales-page-set/maejang/*")
        outputFilePath = hadoop_url+"/moneymall/tsv/sales-page-set/current/"
    
	spcshopDF = spcshopRDD.filter(line => line.contains("\t")).map(_.split("\t"))
	.map(c => (
            c(0).toInt,c(1),c(2),c(3),c(4),c(5),c(6),c(7)
        )).toDF("gbn","mall","maejang","maejangDetail","url","param","device","dt")

	pagesetDF = pagesetRDD.filter(line => line.contains("\t")).map(_.split("\t"))
	.map(c => (
        	c(0).toInt,c(1),c(2),c(3),
	        {
        	    if (c(6).contains("mobile app")) {

                	    if (c(0).equals("1") && !c(4).contains("SSG_")) "SSG_"+c(4).replaceAll("\\s+$", "")
	                    else if (c(0).equals("2") && !c(4).contains("EM_")) "EM_"+c(4).replaceAll("\\s+$", "")
        	            else if (c(0).equals("3") && !c(4).contains("TR_")) "TR_"+c(4).replaceAll("\\s+$", "")
                	    else if (c(0).equals("4") && !c(4).contains("BM_")) "BM_"+c(4).replaceAll("\\s+$", "")
	                    else if (c(0).equals("5") && !c(4).contains("SM_")) "SM_"+c(4).replaceAll("\\s+$", "")
        	            else if (c(0).equals("6") && !c(4).contains("SD_")) "SD_"+c(4).replaceAll("\\s+$", "")
                	    else if (c(0).equals("7") && !c(4).contains("TV_")) "TV_"+c(4).replaceAll("\\s+$", "")
	                    else if (c(0).equals("8") && !c(4).contains("HD_")) "HD_"+c(4).replaceAll("\\s+$", "")
        	            else if (c(0).equals("10") && !c(4).contains("BT_")) "BT_"+c(4).replaceAll("\\s+$", "")
                	    else if (c(0).equals("11") && !c(4).contains("SIV_")) "SIV_"+c(4).replaceAll("\\s+$", "")
                	    else if (c(0).equals("12") && !c(4).contains("STF_")) "STF_"+c(4).replaceAll("\\s+$", "")
	                    else c(4).replaceAll("\\s+$", "")
        	    } else {
                	    c(4).replaceAll("\\s+$", "")
	            }
	        },      //url 수정
        	c(5),c(6),c(7)
	)).toDF("gbn","mall","maejang","maejangDetail","url","param","device","dt")

    }

    /////////////////////// MAIN 함수 파트 /////////////////////////////////
    def main(args:Array[String]) {
	val t0 = System.nanoTime()
	init(args)
     	inline(spcshopDF, pagesetDF, outputFilePath)
	var t1 = System.nanoTime()
	println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
	println ("============================================================================")
    }

    /////////////////////// SUB 함수 파트 //////////////////////////////////
    def inline(spcshopDF:org.apache.spark.sql.DataFrame, pagesetDF:org.apache.spark.sql.DataFrame, outputFilePath:String) {
	spcshopDF.registerTempTable("spcshop")
	pagesetDF.registerTempTable("pageset")

	val query1 = """
		SELECT T.gbn, T.mall, MAX(T.maejang), MAX(T.maejangDetail), T.url, T.param, T.device, MAX(T.dt)
		FROM (
		    SELECT 
		        if (A.gbn is null, B.gbn, A.gbn) gbn, 
		        if (A.mall is null, B.mall, A.mall) mall,
		        if (A.maejang is null, B.maejang, A.maejang) maejang,
		        if (A.maejangDetail is null, B.maejangDetail, A.maejangDetail) maejangDetail,
		        if (A.url is null, B.url, A.url) url,
		        if (A.param is null, B.param, A.param) param,
		        if (A.device is null, B.device, A.device) device,
		        if (A.dt is null, B.dt, A.dt) dt
		    FROM pageset A
		    FULL OUTER JOIN spcshop B
		    ON A.url = B.url AND A.param = B.param AND A.device = B.device 
		) T
		GROUP BY T.gbn, T.mall, T.url, T.param, T.device
		ORDER BY T.gbn, T.mall, MAX(T.maejang), MAX(T.maejangDetail), T.device DESC
	"""

	var result1 = sqlContext.sql(query1)
	result1.map(c=> "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s".format(c(0),c(1),c(2),c(3),c(4),c(5),c(6),c(7))).coalesce(1).saveAsTextFile(outputFilePath)

    }

}
