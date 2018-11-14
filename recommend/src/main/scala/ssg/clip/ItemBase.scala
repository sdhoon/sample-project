package ssg.clip


import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import ssg.util.Reader

/**
  * Created by 131326 on 2017-01-24.
  */

class ExecutorItemBase extends Serializable{
  private var hiveContext : HiveContext = _
  private var sc : SparkContext = _

  def init() {
    sc = new SparkContext()
    hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  }

  def run() {
    val reader = new Reader(sc, hiveContext)

   /* val cassandraItembase = sc.cassandraTable("recommend","itembase")
      .select("item_id", "site_no", "recommend_item_id", "reg_dts", "score")

    cassandraItembase.map( row => {
        row.getStringOption("item_id").getOrElse("") + "," +
        row.getStringOption("site_no").getOrElse("") + "," +
        row.getStringOption("recommend_item_id").getOrElse("") + "," +
        row.getStringOption("reg_dts").getOrElse("") + "," +
        row.getStringOption("score").getOrElse("")
      }
    ).saveAsTextFile(reader.getYarnHDFSURI + "/moneymall/recommend/itembase/cassandra/")*/

    val itembase = reader.getItemBase
    itembase.registerTempTable("itembase")
    val itembaserank = hiveContext.sql(SQLMaker.getItemBaseByRank())

    itembaserank.map(line => "%s,%s,%s,%s".format(line(0), line(1), line(2), line(3)))
      .coalesce(1)
      .saveAsTextFile(reader.getYarnHDFSURI() + "/moneymall/recommend/itembase/campaign/")


    sc.stop()
  }


  def checkNull(value:String): String = {
    if(value == null){
      ""
    }else{
      value
    }
  }
}


object ItemBase {
  def main(args: Array[String]) {
    val executor = new ExecutorItemBase
    executor.init()
    executor.run()
  }
}
