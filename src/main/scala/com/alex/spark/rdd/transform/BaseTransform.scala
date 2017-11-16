package com.alex.spark.rdd.transform
/**
  * Created by admin on 2017/9/18.
  */
import com.alex.spark.util.SparkEnv
object BaseTransform{
  def mapFun(): Unit ={
    val rdd = SparkEnv.sc.parallelize(1 to 5)
    val mapRDD = rdd.map(x => x + 2)
    mapRDD.foreach(x => print(x+" "))
  }

  def flatMapFun(): Unit ={
    val list = List((1,2),(2,3),(3,5))
    val rdd = SparkEnv.sc.parallelize(1 to 5)
    val flatMapRDD = rdd.flatMap(x => (1 to x))
    flatMapRDD.foreach(x => print(x+" "))
  }

  def partitionsFun(iter : Iterator[(String,String)]) : Iterator[String] = {
    var woman = List[String]()
    while (iter.hasNext){
      val next = iter.next()
      next match {
        case (_,"female") => woman = next._1 :: woman
        case _ =>
      }
    }
    return  woman.iterator
  }

  def mapPartitionsFun(): Unit = {
    val l = List(("kpop","female"),("zorro","male"),("mobin","male"),("lucy","female"))
    val rdd = SparkEnv.sc.parallelize(l,2)
    val mp = rdd.mapPartitions(partitionsFun)
    println(s">>>>>>>>>>>>${mp.collect().mkString(" ")}<<<<<<<<<<<<<")
    //mp.collect.foreach(x => (print(x +" "))) //将分区中的元素转换成Aarray再输出
  }

  def partitionWithIndexFun(x : Int, iter :Iterator[Int])={
    var result = List[String]()
    var i : String = ""
    while (iter.hasNext) {
      i =  i + " " + iter.next()
    }
    result.::(x + "|" + i).iterator
  }

  def mapPartitionWithIndexFun():Unit = {
    val rdd1 = SparkEnv.sc.makeRDD(1 to 5,2)
    val rdd2 = rdd1.mapPartitionsWithIndex{
      (x, iter) => {
        var result = List[String]()
        var i = 0
        while (iter.hasNext){
          i += iter.next()
        }
        result.::(x + "|" + i).iterator
      }
    }
    val rdd3 = rdd1.mapPartitionsWithIndex(partitionWithIndexFun)
    rdd3.collect().foreach(println)
  }

  def sampleFun():Unit={
    val rdd = SparkEnv.sc.parallelize(1 to 10)
    val sample1 = rdd.sample(true,0.5,3)
    println(s">>>>>>>>>>>>${sample1.collect().mkString(" ")}<<<<<<<<<<<<<")
    sample1.collect.foreach(x => print(x + " "))
  }

  def unionFun():Unit = {
    val rdd1 = SparkEnv.sc.parallelize(1 to 4)
    val rdd2 = SparkEnv.sc.parallelize(3 to 5)
    val unionRDD = rdd1.intersection(rdd2) //union  intersection
    println(s">>>>>>>>>>>>${unionRDD.collect().mkString(" ")}<<<<<<<<<<<<<")
    unionRDD.collect.foreach(x => print(x + " "))
  }

  def interSectionFun(): Unit = {
    val rdd1 = SparkEnv.sc.parallelize(1 to 3)
    val rdd2 = SparkEnv.sc.parallelize(3 to 5)
    val unionRDD = rdd1.intersection(rdd2)
    println(s">>>>>>>>>>>>${unionRDD.collect().mkString(" ")}<<<<<<<<<<<<<")
  }

  def dinstinctFun(): Unit = {
    val list = List(1,1,2,5,2,9,6,1)
    val distinctRDD = SparkEnv.sc.parallelize(list)
    val unionRDD = distinctRDD.distinct() //union   intersection
    println(s">>>>>>>>>>>>${unionRDD.collect().mkString(" ")}<<<<<<<<<<<<<")
  }

  def cartesianFun(): Unit = {
    val rdd1 = SparkEnv.sc.parallelize(1 to 3)
    val rdd2 = SparkEnv.sc.parallelize(2 to 5)
    val cartesianRDD = rdd1.cartesian(rdd2)
    println(s">>>>>>>>>>>>${cartesianRDD.collect().mkString(" ")}<<<<<<<<<<<<<")
  }

  def coalesceFun():Unit = {
    val rdd = SparkEnv.sc.parallelize(1 to 16,4)
    rdd.foreachPartition(iter => print(iter.toList+ " | "))
    val coalesceRDD = rdd.coalesce(3, false)   //当suffle的值为false时，不能增加分区数(如分区数不能从5->7)
    // val coalesceRDD = rdd.coalesce(5,true)
    println("重新分区后的分区个数:"+coalesceRDD.partitions.size)
    println("RDD依赖关系:"+coalesceRDD.toDebugString)
    coalesceRDD.foreachPartition(iter => print(iter.toList + " | "))
  }

  def repartitionFun(): Unit = {
    val rdd = SparkEnv.sc.parallelize(1 to 16,4)
    val repartitionRDD = rdd.repartition(2)
    println(s">>>>>>>>>>>>${repartitionRDD.foreachPartition(iter => iter.toList + " | ")}<<<<<<<<<<<<<")
    repartitionRDD.foreachPartition(iter => print(iter.toList + " | "))
  }

  def glomFun(): Unit = {
    val rdd = SparkEnv.sc.parallelize(1 to 16,4)
    val glomRDD = rdd.glom() //RDD[Array[T]]
    glomRDD.foreach(rdd => println(rdd.getClass.getSimpleName))
  }

  def randomSplitFun(): Unit = {
    val rdd = SparkEnv.sc.parallelize(1 to 10)
    val randomSplitRDD = rdd.randomSplit(Array(1.0,2.0,7.0))
    randomSplitRDD(0).foreach(x => print(x +" "))
    randomSplitRDD(1).foreach(x => print(x +" "))
    randomSplitRDD(2).foreach(x => print(x +" "))
  }
}