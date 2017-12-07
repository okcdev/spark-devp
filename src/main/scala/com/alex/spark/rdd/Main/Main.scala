package com.alex.spark.rdd.Main
import com.alex.spark.dataMng.{ProData, processLogs}
import com.alex.spark.rdd.action.BaseAction
import com.alex.spark.sql.{CsvSql, JsonSql}
import com.alex.spark.util.SparkEnv
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
    val spark = SparkSession
      .builder()
      .appName("REST")
      .config(conf)
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

    ProData.loadCsv()

   // CsvSql.readCsv(array)
    //println(s">>>>>>>>>>>>>>*****************<<<<<<<<<<<<<")
    SparkEnv.sc.stop()
  }
}