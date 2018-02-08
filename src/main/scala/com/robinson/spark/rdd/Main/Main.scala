package com.robinson.spark.rdd.Main

import com.robinson.spark.mllib.ClassifyRegression
import com.robinson.spark.parseCsv.ReadCsv
import com.robinson.spark.sql.LoadHive
import com.robinson.spark.util.SparkEnv
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by fengtao.xue on 2017/9/18.
  */

object Main{

  def initSpark(spark:SparkSession): Unit ={
    SparkEnv.initialize(spark)
  }

  def main(array: Array[String]): Unit ={
    val conf = new SparkConf()
      .setMaster("local")
      .set("spark.sql.warehouse.dir","hdfs://us01:9000/hive/warehouse")
    val spark = SparkSession
      .builder()
      .appName("spar-devp-app")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    //initialize sparksession
    initSpark(spark)
    //transform demo
    println(s">>>>>>>>>>>>>>*******************<<<<<<<<<<<<<")
    //println(s">>>>>>>>>>>>>>RDD transform start<<<<<<<<<<<<<")
    /*
    BaseTransform.mapFun()
    BaseTransform.flatMapFun()
    BaseTransform.mapPartitionsFun()
    BaseTransform.mapPartitionWithIndexFun()
    BaseTransform.sampleFun()
    BaseTransform.unionFun()
    BaseTransform.interSectionFun()
    BaseTransform.dinstinctFun()
    BaseTransform.cartesianFun()
    BaseTransform.coalesceFun()
    BaseTransform.repartitionFun()
    BaseTransform.glomFun()
    BaseTransform.randomSplitFun()
    */
    /*
    KVTransform.mapValueFun()
    KVTransform.flatMapValueFun()
    KVTransform.combinerByKeyFun()
    KVTransform.foldByKeyFun()
    KVTransform.reduceByKeyFun()
    KVTransform.groupByKeyFun()
    KVTransform.sortByKeyFun()
    KVTransform.cogroupFun()
    KVTransform.joinFun()
    */
    //println(s">>>>>>>>>>>>>>RDD transform end<<<<<<<<<<<<<")
    /*
    println(s">>>>>>>>>>>>>>RDD action start<<<<<<<<<<<<<")
    BaseAction.baseAction()
    BaseAction.kvAction()
    println(s">>>>>>>>>>>>>>RDD action end<<<<<<<<<<<<<")
    */
    /*
    println(s">>>>>>>>>>>>>> read json start<<<<<<<<<<<<<")
    JsonSql.loadJson()
    //processLogs.joinedFun()
    println(s">>>>>>>>>>>>>> read json end<<<<<<<<<<<<<")
    */
    /*println(s">>>>>>>>>>>>>> loading from ftp start<<<<<<<<<<<<<")
    ProData.loadTextFromFtp()
    println(s">>>>>>>>>>>>>> loading from ftp end<<<<<<<<<<<<<")*/

//    ProData.callHttp()

//    ProData.loadCsv()

    ClassifyRegression.LogistReg

    /*
    println(s">>>>>>>>>>>>>> load csvfile start<<<<<<<<<<<<<")
    val path:String = "."//当前工程根路径
    ReadCsv.loadCsvFile(path)
    println(s">>>>>>>>>>>>>> load csvfile end<<<<<<<<<<<<<")
    */

    /*println(s">>>>>>>>>>>>>> load2hive start<<<<<<<<<<<<<")
    LoadHive.load2hive()
    println(s">>>>>>>>>>>>>> load2hive end<<<<<<<<<<<<<")*/

    SparkEnv.sc.stop()
  }
}