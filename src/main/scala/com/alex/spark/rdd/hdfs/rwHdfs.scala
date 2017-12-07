package com.alex.spark.rdd.hdfs

import com.alex.spark.util.SparkEnv
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by fengtao.xue on 2017/9/19.
  */
object rwHdfs {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")
    val spark = SparkSession.builder().config(conf).appName("rwHdfs").getOrCreate()
    SparkEnv.initialize(spark)
    val inputRDD = SparkEnv.sc.textFile("/home/app/data/test/rdd1.csv")
    val outputRDD = inputRDD.map(lines => lines.split("\t"))
    outputRDD.collect().foreach(println)
    outputRDD.saveAsTextFile("hdfs://us01:9000/spark/rdd")
    SparkEnv.sc.stop()
  }
}
