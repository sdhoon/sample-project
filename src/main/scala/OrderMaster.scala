
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util._
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
 * @author Y.G 2015-06-05
 *
 */
object OrderMaster {
  
    val sparkConf = new SparkConf().setAppName("OrderMaster")
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
  
    var orderInfo:RDD[String] = null
    var cartMappingInfo:RDD[String] = null
    var orderCount:Long = 0
    var cartCount:Long = 0
    var resultCount:Long = 0
  
    var date:String=""
    var date_hour:String=""
    var date_str:String=""
    var date_file:String=""

    var cartMappingInfo_path:String=""
    val hadoop_url = "hdfs://bdnodea001.prod.moneymall.ssgbi.com:9000"
  
    val year = new SimpleDateFormat("yyyy")
    val year2 = new SimpleDateFormat("yy")
    val month = new SimpleDateFormat("MM")
    val dayStr = new SimpleDateFormat("dd")
    val hour = new SimpleDateFormat("HH")
    var todayDate:Date = new Date();
    var c = Calendar.getInstance()
    
    def init(args:Array[String]){
  	todayDate = c.getTime()
    
    	if(args.length > 0){//소급 파라미터가 있을 경우
       	    setCalendar(args(0))
    	}

	var tempDate = ""
	for (i <- 1 to 384) {
      	    c.add(Calendar.HOUR_OF_DAY,-1)
      	    var today = c.getTime()
      	    date = "%s-%s-%s".format(year2.format(today),month.format(today),dayStr.format(today))
            date_hour = "%s".format(hour.format(today))
            date_file = "%s-%s-%s_%s".format(year.format(today),month.format(today),dayStr.format(today),hour.format(today))
      
      	    if (i == 1) {
        	date_str = "%s-%s-%s %s".format(year.format(today),month.format(today),dayStr.format(today),hour.format(today))
        	println("Start : %s-%s-%s %s".format(year.format(today),month.format(today),dayStr.format(today),hour.format(today)))            
        	orderInfo = sc.textFile(hadoop_url + "/moneymall/tsv/order-event-row/"+date+"/"+date_hour+"/*")
      	    }
      
      
      	    if(cartMappingInfo == null){
		tempDate = date
		cartMappingInfo = sc.textFile(hadoop_url + "/moneymall/tsv/previous-sales/"+date+"/*/*")
            	println(hadoop_url + "/moneymall/tsv/previous-sales/"+date+"/*/*")
      	    } else {

		if (tempDate != date) {
			tempDate = date
			cartMappingInfo ++= sc.textFile(hadoop_url + "/moneymall/tsv/previous-sales/"+date+"/*/*")
			println(hadoop_url + "/moneymall/tsv/previous-sales/"+date+"/*/*")
		}
            }
    	}

	//cartMappingInfo = sc.textFile(cartMappingInfo_path)
    }
  
  
  case class ORDER(ordNo:String, ordItemSeq:String, cartItemSiteNo:String, masterCartId:String, cartId:String, itemId:String, uitemId:String, unitPrice:String, ordQty:String, ckWhere:String, ts:String, tgtMediaCd:String, browser:String, ip:String, sessionId:String, dmid:String,pcid:String, mbrid:String)
  case class CART(cartItemSiteNo:String, itemId:String, uitemId:String, masterCartId:String, cartId:String, cartTimeStamp:String, allRow:String)
  
  def getData(args:Array[String]) {
    
    val rowOrderRDD = orderInfo.map(_.split("\t")).map(p => ORDER(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9),p(10),p(11),p(12),p(13),p(14),p(15),p(16),p(17))).toDF()
    val rowCartRDD  = cartMappingInfo.map(_.split("\t")).map(p => CART(p(0), p(1), p(2), p(4), p(5), p(6), p.mkString("\t"))).toDF()
    
    rowOrderRDD.registerTempTable("ORDER_EVENT")
    rowCartRDD.registerTempTable("CART_MASTER")
    
    
    var query = """
                SELECT 
                       MAX(cartTimeStamp) AS cartTimeStamp
                     , cartItemSiteNo
                     , itemId
                     , uitemId
                     , masterCartId
                     , cartId                             
                FROM CART_MASTER
                GROUP BY cartItemSiteNo, itemId, uitemId, masterCartId, cartId
                """
     val cartMasterGroup = sqlContext.sql(query)
     cartMasterGroup.registerTempTable("CART_MASTER_GROUP")
     
     query = """
              SELECT
                        A.ordNo
                      , A.ordItemSeq
                      , A.ordQty
                      , A.unitPrice
                      , A.ts
                      , A.ckWhere
                      , A.dmid
                      , A.tgtMediaCd
                      , A.browser
                      , A.ip
                      , A.sessionId
                      , A.pcid
                      , A.mbrid
                      , B.allRow
              FROM ORDER_EVENT A
              LEFT OUTER JOIN
              (
                SELECT   
                         CA.itemId
                       , CA.uitemId
                       , CA.masterCartId
                       , CA.cartId
                       , CA.allRow                            
                FROM CART_MASTER CA
                JOIN
                CART_MASTER_GROUP CMG
                ON  CA.cartItemSiteNo = CMG.cartItemSiteNo
                AND CA.itemId         = CMG.itemId
                AND CA.uitemId        = CMG.uitemId
                AND CA.masterCartId   = CMG.masterCartId
                AND CA.cartId         = CMG.cartId
                AND CA.cartTimeStamp  = CMG.cartTimeStamp
              ) B
              ON   A.itemId       = B.itemId
              AND  A.uitemId      = B.uitemId
              AND  A.masterCartId = B.masterCartId
              AND  A.cartId       = B.cartId
            """
     val results = sqlContext.sql(query).map(col => {
	var everyClickString:String = "" 
	if (col(13) == null || col(13).toString.split("\t").length < 10) everyClickString = "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t.\tEND\t0" 
	else everyClickString = col(13).toString
       "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s".format(col(0),col(1), col(2), col(3), col(4), col(5), col(6), col(7), col(8), col(9), col(10), col(11), col(12), everyClickString)
    })
    
   if(args.length > 0){//소급 파라미터가 있을 경우
     setCalendar(args(0))     
    }else{
      c = Calendar.getInstance()
    }
    
    c.add(Calendar.HOUR_OF_DAY,-1)
    var today = c.getTime()
    date = "%s-%s-%s".format(year2.format(today),month.format(today),dayStr.format(today))
    date_hour = "%s".format(hour.format(today))
    
    println("date : " + date)
    println("date_hour : " + date_hour)
    println(hadoop_url + "/moneymall/tsv/order-master/"+date+"/"+date_hour+"/")
    results.coalesce(1).saveAsTextFile(hadoop_url + "/moneymall/tsv/order-master/"+date+"/"+date_hour+"/")
    
    //orderCount = rowOrderRDD.count()
    //cartCount = rowCartRDD.count()
    //resultCount = results.count()
  }
  
  def setCalendar(dateString:String) {    
    var sdf = new SimpleDateFormat("yy-MM-dd-HH")
    c.setTime(sdf.parse(dateString))
    c.add(Calendar.HOUR_OF_DAY,+1)
  }
  
 
  def main(args:Array[String]) {
    
    val startTime = System.nanoTime()
    
    println ("============================================================================")
    init(args)
    getData(args)    
    val endTime = System.nanoTime()
    //println("orderCount    : " + orderCount)
    //println("cartCount     : " + cartCount)
    //println("resultCount   : " + resultCount)
    println ("Elapsed Time : " + ( endTime - startTime )/1000000000 + "sec")
    println ("============================================================================")
  }
  
  //Below code is for Spark 1.3
  /*import org.apache.spark.sql.DataFrame
  
  def getData(args:Array[String]) {
    
    val sqlContext = new SQLContext(sc)
    
    //define string schema
    val schemaOrderString = "ordNo ordItemSeq cartItemSiteNo masterCartId cartId itemId uitemId unitPrice ordQty ckWhere"
    val schemaCartString = "itemId uitemId masterCartId cartId allRow"
    
    import org.apache.spark.sql.Row;
    import org.apache.spark.sql.types.{StructType,StructField,StringType};
    
    val schemaOrder = StructType(schemaOrderString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val schemaCart  = StructType(schemaCartString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    
    val rowOrderRDD = orderInfo.map(_.split("\t")).map(p => Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9)))
    val rowCartRDD  = cartMappingInfo.map(_.split("\t")).map(p => Row(p(1), p(2), p(4), p(5), p.mkString("\t")))
    
    val orderDataFrame = sqlContext.createDataFrame(rowOrderRDD, schemaOrder)
    orderDataFrame.registerTempTable("ORDER_EVENT")
    
    val cartDataFrame = sqlContext.createDataFrame(rowCartRDD, schemaCart)
    cartDataFrame.registerTempTable("CART_MASTER")
    
    val query = """
                SELECT
                      A.ordNo, A.ordItemSeq, A.cartItemSiteNo, A.masterCartId, A.cartId, A.itemId, A.uitemId, A.unitPrice, A.ordQty, A.ckWhere, B.allRow 
                FROM ORDER_EVENT A
                     LEFT OUTER JOIN
                     CART_MASTER B
                ON   A.itemId       = B.itemId
                AND  A.uitemId      = B.uitemId
                AND  A.masterCartId = B.masterCartId
                AND  A.cartId       = B.cartId
                """
    val results = sqlContext.sql(query)
    results.save("hdfs://master001.prod.moneymall.ssgbi.com:9000/moneymall/test/result", org.apache.spark.sql.SaveMode.Overwrite)
    
  }*/
}

