package com.alex.spark.dataMng

import java.io.{StringReader, StringWriter}

import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
import com.alex.spark.util.SparkEnv
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.SparkSession
import org.codehaus.jackson.map.ObjectMapper
import org.eclipse.jetty.client.{ContentExchange, HttpClient}

/**
  * loading text from ftp
  * Created by fengtao.xue on 2017/12/7.
  */
object ProData {

  case class Student(name: String, favouriteSport: String)
  case class Person(name:String, sex:String, age:Int)

  def loadTextFromFtp():Unit = {
    val file = SparkEnv.sc.textFile("ftp://anony:alex@us01/home/data/ls-LR.gz")
    println(file.collect().mkString("\n"))
  }

  def loadCsv():Unit={
    val inputFile = "/fengtao.xue/data/csv/1.csv";
    val input = SparkEnv.sc.textFile(inputFile)
    val result = input.map { line =>
      val reader = new CSVReader(new StringReader(line));
      reader.readNext();
    }
    val student = result.map(x => Student(x(0), x(1)))
    val basketballLovers = student.filter(student => student.favouriteSport == "basketball")
    basketballLovers.map(student => List(student.name, student.favouriteSport).toArray).mapPartitions { student =>
      val stringWriter = new StringWriter();
      val csvWriter = new CSVWriter(stringWriter);
      csvWriter.writeAll(student.toList)
      Iterator(stringWriter.toString)
    }.saveAsTextFile("/fengtao.xue/data/csv/output")
  }

  def loadJson():Unit={
    val input = SparkEnv.sc.textFile("/fengtao.xue/data/people.json")
    val result = input.mapPartitions(records => {
      //mapper object created on each executor node
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      records.flatMap(record => {
        try{
          Some(mapper.readValue(record, classOf[Person]))
        }catch {
          case e: Exception => None
        }
      })
    }, true)
    result.filter(_.sex).mapPartitions(records => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      records.map(mapper.writeValueAsString(_))
    })
      .saveAsTextFile("fengtao.xue/data/output")
  }

  def FlumeInput(): Unit ={

  }

  def callHttp(): Unit = {
    val input = SparkEnv.sc.parallelize(List("KK6JKQ", "Ve3UoW", "kk6jlk", "W6BB"))
    val result = input.mapPartitions {
      signs =>
        val client = new HttpClient()
        client.start()
        signs.map { sign =>
          val exchange = new ContentExchange(true);
          //exchange.setURL(s"http://qrzcq.com/call/${sign}")
          exchange.setURL(s"http://baidu.com")
          client.send(exchange)
          exchange
        }.map { exchange =>
          exchange.waitForDone();
          exchange.getResponseContent()
        }
    }
    println(result.collect().mkString(","))
  }
}
