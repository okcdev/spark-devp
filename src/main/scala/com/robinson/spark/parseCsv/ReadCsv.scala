package com.robinson.spark.parseCsv

import com.robinson.spark.util.SparkEnv

/**
  * Created by admin on 2018/1/13.
  */
object ReadCsv {

  def loadCsvFile(path:String):Unit ={
    val sqlContext = SparkEnv.sqlContext
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(path + "/data/vehiclemodels.csv")

    val selectModel = df.select("versionFullName", "subModelName", "vehicleTypeName","modelYear","launchDate", "msrp")
    println(s"************* vehiclemodels count:" + selectModel.count())
    selectModel.show();
    selectModel.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(path + "/data/select_model")
  }

}
