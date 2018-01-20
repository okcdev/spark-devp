package com.robinson.spark.sql

import com.robinson.spark.util.SparkEnv
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by fengtao.xue on 2018/1/20.
  */
object LoadHive {

  def load2hive():Unit={
    val hiveContext = new HiveContext(SparkEnv.sc)
    val sqlStr = "select manfname,brandname,networkname,dealergroupname,cityname from mid_dealer_networks limit 20"
    hiveContext.sql("use mydb")
    val result = hiveContext.sql(sqlStr).toDF()
    println(s"*********result count: " + result.count())
    result.show()
  }

}
