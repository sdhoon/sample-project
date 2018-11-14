package ssg.collaboration

import java.io.Serializable
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage._
import org.apache.spark.{SparkConf, SparkContext}
import ssg.util.{Reader, _}

import scala.collection.immutable.HashMap

/**
  * Created by moneymall on 01/07/16.
  */
object ItemBasedInputMaker {
  def main(args:Array[String]) {
    val executor = new ExecutorCF()
    executor.init()

    println("#####" + args + "#####")
    executor.run(args(1).toInt, args(0), args(2))
  }

}

class ExecutorCF extends Serializable{
  private var hiveContext : HiveContext = _
  private var sc : SparkContext = _

  def init() {
    val sparkConf = new SparkConf()
      .setAppName("recommend.Recommend")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.local.dir","/data01/spark_tmp/")
      .set("spark.buffer.pageSize", "16m")
      .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
      .set("spark.executor.uri","hdfs://master001p27.prod.moneymall.ssgbi.com:9000/user/spark/spark-1.6.1-bin-hadoop2.6.tgz")

    sc = new SparkContext(sparkConf)
    hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.setConf("spark.sql.tungsten.enabled", "false")

  }

  def run(day:Int, date:String, types:String) {

    val hiveContext1 = hiveContext
    val sd = new SimpleDateFormat("yy-MM-dd")

    val reader = new Reader(sc, hiveContext)

    val dt = date.split("-")
    val calendar = Calendar.getInstance()
    val tempDate = new Date(100+dt(0).toInt,dt(1).toInt-1,dt(2).toInt,0,0,0)
    calendar.setTime(tempDate)


    import hiveContext1.implicits._

    for(i <- 0 to day - 1){
      println("############### ItemBasedInputMaker START : " +  sd.format(calendar.getTime) + " ItemBasedInputMaker ###############")



      var dataDf = hiveContext.emptyDataFrame
      if(types.equals("click")){
        val click = reader.getClick(calendar)
        click.registerTempTable("CLICK")

        dataDf = hiveContext.sql(SQLMaker.getClickCountById())
        dataDf.registerTempTable("CLICK")
        //click1.show()

      }else if(types.equals("order")){
        val orderEvent = reader.orderEventRow(sd.format(calendar.getTime))
        orderEvent.registerTempTable("ORDER")
        //orderEvent.show

        dataDf = hiveContext.sql(SQLMaker.getOrdCountById())
        dataDf.registerTempTable("ORDER")
        //dataDf.show()///

      }


      val userRdd = dataDf.map(user => user(0).toString).persist(StorageLevel.DISK_ONLY)
      val userKeys = userRdd.distinct.collect
      var maxVal = 0;
      val userMap = HashMap(userKeys.zip(Stream from maxVal.toInt) : _*)
      maxVal = userMap.valuesIterator.max + 1
      val userBR = sc.broadcast(userMap)

      val data = dataDf.map {
        t =>
          val userId = userBR.value.get(t(0).toString).head
          val itemId = t(1).toString
          val cnt = t(2).toString.toLong
          Collaboration(userId.toString, itemId, cnt, t(3).toString)
      }.persist(StorageLevel.DISK_ONLY).toDF()
      //data.show()


      data.map(line => "%s\t%s\t%s\t%s".format(line(0), line(1), line(2), line(3)))
        .coalesce(1)
        .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/cf/itembased/input/" + types + "/" + sd.format(calendar.getTime) + "/")

      calendar.add(Calendar.DATE, -1)
    }
  }
}
