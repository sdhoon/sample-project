package ssg.collaboration

import java.io.Serializable
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import com.datastax.spark.connector._
import ssg.util.Itembase2
import java.util.UUID
import com.datastax.driver.core.utils.UUIDs

/**
  * Created by 131326 on 2017-04-05.
  */
object CopyItemBaseTable {

  def main(args:Array[String]) {
    val executorItemBase = new ExecutorItemBase
    executorItemBase.init()
    executorItemBase.run()
  }
}

class ExecutorItemBase extends Serializable{

  private var hiveContext : HiveContext = _
  private var sc : SparkContext = _

  def init() {

    sc = new SparkContext()
    hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  }

  def run() = {
    val itembase = sc.cassandraTable("recommend", "itembase")
    val data = itembase
      .map(attr => Itembase2(
        attr.getString("item_id"),
        attr.getString("site_no"),
        attr.getString("method"),
        attr.getString("recommend_item_id"),
        attr.getUUIDOption("reg_dts"),
        attr.getDouble("score")))
    data.saveToCassandra("recommend", "itembase2")
  }

  def checkNull(value:UUID): UUID = {
    if(value == None){
      UUIDs.timeBased()
    }else{
      value
    }
  }

}


