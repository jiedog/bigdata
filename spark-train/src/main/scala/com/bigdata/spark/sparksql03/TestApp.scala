package com.bigdata.spark.sparksql03

import org.apache.spark.sql.SparkSession

/**
  * @Author 种庆凯
  * @Date ${date} ${time}
  */
object TestApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("TestApp").getOrCreate()
    val data=spark.read.format("com.bigdata.spark.sparksql03.DefaultSource").load("spark-train/input/baidu.log")
      data.select(data("time"),data("ip"),data("url"))
      .filter(data("url")==="v3.com")

          .show(100)

    print(Runtime.getRuntime.availableProcessors())
    spark.stop()
  }

}
