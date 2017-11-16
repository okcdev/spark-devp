package com.alex.spark.sql

//import java.io.{StringReader, StringWriter}

import com.alex.spark.util.SparkEnv
//import au.com.bytecode.opencsv.CSVReader
//import au.com.bytecode.opencsv.CSVWriter

import org.eclipse.jetty.client.ContentExchange
import org.eclipse.jetty.client.HttpClient

/**
  * Created by admin on 2017/9/28.
  */
object CsvSql {

  case class Person(name: String, favouriteAnimal: String)

//  def readCsv(args: Array[String]) {
//    if (args.length < 3) {
//      println("Usage: [sparkmaster] [inputfile] [outputfile]")
//      exit(1)
//    }
//    val master = args(0)
//    val inputFile = args(1)
//    val outputFile = args(2)
//    val input = SparkEnv.sc.textFile(inputFile)
//    val result = input.map { line =>
//      val reader = new CSVReader(new StringReader(line));
//      reader.readNext();
//    }
//    val people = result.map(x => Person(x(0), x(1)))
//    val pandaLovers = people.filter(person => person.favouriteAnimal == "panda")
//    pandaLovers.map(person => List(person.name, person.favouriteAnimal).toArray).mapPartitions { people =>
//      val stringWriter = new StringWriter();
//      val csvWriter = new CSVWriter(stringWriter);
//      csvWriter.writeAll(people.toList)
//      Iterator(stringWriter.toString)
//    }.saveAsTextFile(outputFile)
//  }

  def callHttp(): Unit = {
    val input = SparkEnv.sc.parallelize(List("KK6JKQ", "Ve3UoW", "kk6jlk", "W6BB"))
    val result = input.mapPartitions {
      signs =>
        val client = new HttpClient()
        client.start()
        signs.map { sign =>
          val exchange = new ContentExchange(true);
          exchange.setURL(s"http://qrzcq.com/call/${sign}")
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
