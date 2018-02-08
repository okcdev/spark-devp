package com.robinson.spark.sql

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Created by fengtao.xue on 2018/2/5.
  */
object LoadHbase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tablename = "zy-test"
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set(HConstants.ZOOKEEPER_QUORUM, "IP:PORT")
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase-unsecure");
    //val hbaseContext = new HBaseContext(sc, conf)
    //创建一个扫描对象
    val scan = new Scan
    //val hbaseRdd = hbaseContext.hbaseRDD(TableName.valueOf(tablename), scan)

    // 建立一个数据库的连接
    val conn = ConnectionFactory.createConnection(conf);
    val hAdmin = conn.getAdmin
    if (!hAdmin.tableExists(TableName.valueOf(tablename))) {
      println(tablename + " is not exist");
      return
    }

    var resultSet = Set[String]()
    //获取表
    val table = conn.getTable(TableName.valueOf(tablename))
    // 扫描全表输出结果
    val results = table.getScanner(scan)
    val it: java.util.Iterator[Result] = results.iterator()
    while(it.hasNext) {
      val result = it.next()
      val cells = result.rawCells()
      for(cell <- cells) {
        println("行建:" + new String(CellUtil.cloneRow(cell)))
        println("列族:" + new String(CellUtil.cloneFamily(cell)))
        println("列名:" + new String(CellUtil.cloneQualifier(cell)))
        println("值:" + new String(CellUtil.cloneValue(cell)))
        println("时间戳:" + cell.getTimestamp())
        var str = Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell))
        resultSet += str
        println("---------------------")
      }
    }
    println(resultSet)

    def handleSet(rs : Set[String]): String = {
      var str = ""
      rs.foreach(r => {
        val arr = r.split(":", 2)
        str += "\"" + r +"\":{\"cf\":\"" + arr(0) + "\", \"col\":\"" + arr(1) + "\", \"type\":\"string\"},"
      })

      str.substring(0, str.length-1)
    }

    var columnsDes = handleSet(resultSet)
    println(columnsDes)

    def catalog = s"""{
                     |"table":{"namespace":"default", "name":"${tablename}"},
                     |"rowkey":"key",
                     |"columns":{
                     |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
                     | ${columnsDes}
                     |}
                     |}""".stripMargin

    println("********" + catalog)
    val sqlContext = new SQLContext(sc);

    /*def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog -> cat))
        .format("org.apache.hadoop.hbase.spark")
        .load()
    }
    val df = withCatalog(catalog)
    df.show()*/

    // 关闭资源
    results.close()
    table.close()
    conn.close()

    sc.stop()
  }
}
