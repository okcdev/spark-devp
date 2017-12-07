package com.alex.spark.dataMng

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import com.alex.spark.util.SparkEnv
import org.eclipse.jetty.client.{ContentExchange, HttpClient}

/**
  * loading text from ftp
  * Created by fengtao.xue on 2017/12/7.
  */
object ProData {

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
    println(result.collect())
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
