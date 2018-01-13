package com.robinson.spark.rdd.transform

import com.robinson.spark.util.SparkEnv

/**
  * Created by fengtao.xue on 2017/9/19.
  */
object KVTransform{

  def mapValueFun():Unit = {
    val list = List(("mobin",22),("kpop",20),("lufei",23))
    val rdd = SparkEnv.sc.parallelize(list)
    val mapValuesRDD = rdd.mapValues(x => Seq(x, "male") )
    mapValuesRDD.foreach(println)
  }

  def flatMapValueFun():Unit = {
    val list = List(("mobin",22),("kpop",20),("lufei",23))
    val rdd = SparkEnv.sc.parallelize(list)
    val mapValuesRDD = rdd.flatMapValues(x => Seq(x,"male"))
    mapValuesRDD.foreach(println)
  }

  def combinerByKeyFun():Unit = {
    val people = List(("male", "Mobin"), ("male", "Kpop"), ("female", "Lucy"), ("male", "Lufei"), ("female", "Amy"))
    val rdd = SparkEnv.sc.parallelize(people)
    val combinByKeyRDD = rdd.combineByKey(
      (x: String) => (List(x), 1),
      (peo: (List[String], Int), x: String) => (x :: peo._1, peo._2 + 1),
      (sex1: (List[String], Int), sex2: (List[String], Int)) => (sex1._1 ::: sex2._1, sex1._2 + sex2._2))
    combinByKeyRDD.foreach(println)
    println(combinByKeyRDD.toDebugString)
  }

  def foldByKeyFun():Unit = {
    val people = List(("Mobin", 2), ("Mobin", 1), ("Lucy", 2), ("Amy", 1), ("Lucy", 3))
    val rdd = SparkEnv.sc.parallelize(people)
    val foldByKeyRDD = rdd.foldByKey(2)(_ + _)
    foldByKeyRDD.foreach(println)
  }

  def reduceByKeyFun():Unit = {
    val arr = List(("A",1),("B",2),("A",2),("B",3))
    val rdd = SparkEnv.sc.parallelize(arr)
    val reduceByKeyRDD = rdd.reduceByKey( _ + _ )
    reduceByKeyRDD.foreach(println)
  }

  def groupByKeyFun():Unit = {
    val arr = List(("A",1),("B",2),("A",2),("B",3))
    val rdd = SparkEnv.sc.parallelize(arr)
    val groupByKeyRDD = rdd.groupByKey()
    groupByKeyRDD.foreach(println)
  }

  def sortByKeyFun():Unit = {
    val arr = List(("A",1),("B",2),("A",2),("B",3))
    val rdd = SparkEnv.sc.parallelize(arr)
    val sortByKeyRDD = rdd.sortByKey()
    sortByKeyRDD.foreach(println)
  }

  def cogroupFun():Unit = {
    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    val rdd = SparkEnv.sc.parallelize(arr, 3)
    val rdd1 = SparkEnv.sc.parallelize(arr1, 3)
    val groupByKeyRDD = rdd.cogroup(rdd1)
    groupByKeyRDD.foreach(println)
    println(groupByKeyRDD.toDebugString)
  }

  def joinFun():Unit = {
    val arr = List(("A", 1), ("B", 2))
    val arr1 = List(("A", "A1"), ("B", "B1"),("B", "B1"))

    /*val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3),("C",1))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    leftOuterJoin
    */

    /*
    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"),("C","C1"))
    rightOuterJoin
    */

    val rdd = SparkEnv.sc.parallelize(arr, 3)
    val rdd1 = SparkEnv.sc.parallelize(arr1, 3)
    val rightOutJoinRDD = rdd.fullOuterJoin(rdd1)
    rightOutJoinRDD.foreach(println)
    println(rightOutJoinRDD.toDebugString)
  }
}
