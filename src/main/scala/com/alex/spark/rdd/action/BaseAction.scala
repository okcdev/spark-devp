package com.alex.spark.rdd.action

import com.alex.spark.util.SparkEnv

/**
  * Created by fengtao.xue on 2017/9/19.
  */
object BaseAction {
  def baseAction():Unit = {
    val rdd = SparkEnv.sc.parallelize(1 to 10,2)
    val reduceRDD = rdd.reduce(_ + _)
    val reduceRDD1 = rdd.reduce(_ - _) //如果分区数据为1结果为 -53
    val countRDD = rdd.count()
    val collectRDD = rdd.collect().mkString(" ")
    val firstRDD = rdd.first()
    val takeRDD = rdd.take(5)
    val topRDD = rdd.top(3)
    val takeOrderedRDD = rdd.takeOrdered(3)
    println("func +: "+reduceRDD)
    println("func -: "+reduceRDD1)
    println("count: "+countRDD)
    println("collect: "+collectRDD)
    println("first: "+firstRDD)
    println("take:")
    takeRDD.foreach(x => print(x +" "))
    println("\ntop:")
    topRDD.foreach(x => print(x +" "))
    println("\ntakeOrdered:")
    takeOrderedRDD.foreach(x => print(x +" "))
  }

  def kvAction():Unit = {
    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val rdd = SparkEnv.sc.parallelize(arr,2)
    val countByKeyRDD = rdd.countByKey()
    val collectAsMapRDD = rdd.collectAsMap()
    val lookupRDD = rdd.lookup("A")
    println("countByKey:")
    countByKeyRDD.foreach(print)
    println("\ncollectAsMap:")
    collectAsMapRDD.foreach(print)
    println("\nlookup:")
    lookupRDD.foreach(x => print(x))
  }
}
