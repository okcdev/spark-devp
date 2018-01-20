/**
  * Created by fengtao.xue on 2017/9/19.
  */

package com.robinson.spark.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

object SparkEnv{
  var initialized : Boolean = false
  @transient implicit var spark : SparkSession = null
  @transient implicit var sc : SparkContext = null
  @transient implicit var sqlContext : SQLContext = null

  def initialize(spark : SparkSession) : Boolean = {
    if(!initialized) {
      this.spark = spark
      this.sc = spark.sparkContext
      this.sqlContext = spark.sqlContext
      initialized = true
      true
    }
    else {
      false
    }
  }
}