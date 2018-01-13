package com.robinson.spark.sql

/**
  * Created by fengtao.xue on 2017/9/28.
  */
object CsvSql {

  case class Person(name: String, favouriteAnimal: String)
/*
  def readCsv(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: [sparkmaster] [inputfile] [outputfile]")
      exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)
    val input = SparkEnv.sc.textFile(inputFile)
    val result = input.map { line =>
      val reader = new CSVReader(new StringReader(line));
      reader.readNext();
    }
    val people = result.map(x => Person(x(0), x(1)))
    val pandaLovers = people.filter(person => person.favouriteAnimal == "panda")
    pandaLovers.map(person => List(person.name, person.favouriteAnimal).toArray).mapPartitions { people =>
      val stringWriter = new StringWriter();
      val csvWriter = new CSVWriter(stringWriter);
      csvWriter.writeAll(people.toList)
      Iterator(stringWriter.toString)
    }.saveAsTextFile(outputFile)
  }
*/
}
