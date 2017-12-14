package com.robinson.spark.dataMng

import com.alex.spark.util.SparkEnv
import com.robinson.spark.util.SparkEnv
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.spark_project.jetty.server.Authentication.User

/**
  * Created by fengtao.xue on 2017/9/21.
  */
class UserID(id:String)
class LinkInfo(topic:String, content:String)
object processLogs {
  //初始化代码，从HDFS上的一个Hadoop SequenceFIle中读取用户信息
  //userData中的元素会根据他们被读取时的来源，即HDFS块所在的节点来分布
  //Spark此时无法获知某个特定的UserID对应的记录位于哪个节点上
  def joinedFun():Unit = {
    val userData = SparkEnv.sc.wholeTextFiles("hdfs://us01:9000/spark/newsLogs").partitionBy(new HashPartitioner((2))).persist()
    println("********************: " + userData.collect().mkString(" "))
    val pathNew = "/fengtao.xue/data/newsLogs"

    val events = SparkEnv.sc.wholeTextFiles(pathNew)
    val joined = userData.union(events)
    val lines = joined.reduce((a,b) => ("tmp",a._2 + b._2)).toString()
    println("********************: " + lines)
  }
}

