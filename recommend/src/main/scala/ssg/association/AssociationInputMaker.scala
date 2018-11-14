package ssg.association

import java.io.Serializable
import java.text.SimpleDateFormat

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import ssg.util.{CassandraItemBased, Reader, Schema}
import java.util.Calendar
import java.util.Date
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SQLContext

/**
  * Created by moneymall on 23/06/16.
  */
object AssociationInputMaker {

  def main(args:Array[String]) ={
    val executor = new Executor
    executor.init()



    if(args.size > 1
      && args(0).equals("")
      && args(0).contains("-")){
    }
    executor.run(args(1).toInt, args(0))

  }

}

class Executor extends Serializable{

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
      .set("spark.cassandra.connection.host", "10.203.7.201")
      .set("spark.cassandra.connection.host", "10.203.7.202")

    sc = new SparkContext(sparkConf)
    hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.setConf("spark.sql.tungsten.enabled", "false")
  }

  def run(day:Int, date:String) {

    val reader = new Reader(sc, hiveContext)
    val sd = new SimpleDateFormat("yy-MM-dd")

    val dt = date.split("-")
    val calendar = Calendar.getInstance()
    val tempDate = new Date(100+dt(0).toInt,dt(1).toInt-1,dt(2).toInt,0,0,0)
    calendar.setTime(tempDate)
    println("####" + calendar.getTime)

    val item = reader.item()
    item.registerTempTable("ITEM")


    for(i <- 0 to day - 1){
      val orderEvent = reader.orderEventRow(sd.format(calendar.getTime))
      orderEvent.registerTempTable("DTO")

      val transaction = hiveContext.sql(SQLMaker.getTransaction())

      transaction.map(a => "%s".format(a(1)))
        .coalesce(1)
        .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/associationRule/input/" + sd.format(calendar.getTime))

      calendar.add(Calendar.DATE, -1)
    }
  }

}
