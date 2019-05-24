package com.bigdata.spark.sparksql

//import java.text.SimpleDateFormat
//import java.util.Locale
//
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * @Author 种庆凯
  * @Date ${date} ${time}
  */
//object StuedntApp {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//
//      .master("local[2]").appName("StuedntApp").getOrCreate()
//
//    import spark.implicits._
//
//    val schema = StructType(
//      Array(
//        StructField("cdn",StringType,nullable = true),
//        StructField("region",StringType,nullable = true),
//        StructField("level",StringType,nullable = true),
//        StructField("time",StringType,nullable = true),
//        StructField("ip",StringType,nullable = true),
//        StructField("domain",StringType,nullable = true),
//        StructField("url",StringType,nullable = true),
//        StructField("traffic",LongType,nullable = true)
//      )
//    )
//    val log=spark.sparkContext.textFile("hdfs://spark001:8020/user/hadoop/baidu")
//    val data = log.map(x=>{
//      x.split("\t")
//    }).filter(_.length==8)
//      .map(x=>{
//        val sourceFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z", Locale.ENGLISH)
//        val targetFormat = new SimpleDateFormat("yyyyMMddHHmmss")
//        val time=targetFormat.format(sourceFormat.parse(x(3).substring(1,x(3).length-1)))
//        var traffic=0l
//        try {
//          traffic = x(7).toLong
//        }catch {
//          case e:Exception =>{
//            e.printStackTrace()
//          }
//        }
//        Row(x(0),x(1),x(2),time,x(4),x(6),x(5),traffic)
//      })
//    val logDF= spark.createDataFrame(data ,schema)
//      logDF.show(100)
//    //==========================================
//    //    val log=spark.sparkContext.textFile("input/baidu.log")
//    //    val logDF = log.map(x=>{
//    //      x.split("\t")
//    //    }).filter(_.length==8)
//    //      .map(x=>{
//    //        val sourceFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z", Locale.ENGLISH)
//    //        val targetFormat = new SimpleDateFormat("yyyyMMddHHmmss")
//    //        val time=targetFormat.format(sourceFormat.parse(x(3).substring(1,x(3).length-1)))
//    //        LogInfo(x(0),x(1),x(2),time,x(4),x(6),x(5),x(7))
//    //      }).toDF()
//    //
//    //      .show(100)
//
////    val trafficDF=logDF.select(logDF("domain"),logDF("time").substr(0,8).as("date"),logDF("traffic"))
////    //trafficDF.show()
////    val traffic_dayDF = trafficDF.groupBy(trafficDF("domain"),trafficDF("date")).agg(sum(trafficDF("traffic")).as("day_traffic"))
////    //traffic_dayDF.show()
////    val domain_win = Window.partitionBy("domain").orderBy("date")
////    val result=traffic_dayDF.withColumn("domain_total",sum(traffic_dayDF("day_traffic")).over(domain_win))
////      .withColumn("frist_traffic",first(traffic_dayDF("day_traffic")).over(domain_win))
////      .withColumn("lag_traffic",lag(traffic_dayDF("day_traffic"),1).over(domain_win))
////    result.show()
//    spark.stop()
//  }
//
//
//}
