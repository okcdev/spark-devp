package com.alex.spark.sql

import com.alex.spark.util.SparkEnv
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
  * Created by admin on 2017/9/20.
  */
object JsonSql {

  case class Person(name:String, sex:String, age:Int)

  var mapper:ObjectMapper = null

  def readJson():Unit = {
    val df = SparkEnv.sqlContext.jsonFile("/fengtao.xue/data/people.json")
    df.show()

      df.createOrReplaceTempView("people")
      //df.registerTempTable("people")
      df.persist()

      var sqlQueryStr = "SELECT name FROM people"
      val result = SparkEnv.sqlContext.sql(sqlQueryStr)
      println(s"result: ${result.count()}")
      result.show

//    df.printSchema()
//    printf("select name------")
//    df.select("name").show()
  }

  def loadJson():Unit = {
    //val inputDF = SparkEnv.sqlContext.jsonFile("/fengtao.xue/data/people.json")
    val inputDF = SparkEnv.spark.read.json("/fengtao.xue/data/people.json")
    inputDF.show()
    inputDF.createOrReplaceTempView("people")
    val result = SparkEnv.sqlContext.sql("select name from people")
    result.show()
    val result1 = inputDF.select("age").show()
  }

}
