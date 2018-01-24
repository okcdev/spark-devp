package com.robinson.spark.sql

import com.robinson.spark.util.SparkEnv
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by fengtao.xue on 2018/1/20.
  */
object LoadHive {

  def load2hive():Unit={
    val sc = SparkEnv.sc
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext.sql
    val sqlStr = "select manfname,brandname,networkname,dealergroupname,cityname from mydb.mid_dealer_networks limit 20"
    sql("use mydb")
    val result = sql(sqlStr).toDF()
    println(s"*********result count : " + result.count())
    result.show()
  }

}
