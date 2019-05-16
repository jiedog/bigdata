package com.bigdata.spark.cache

import org.apache.spark.{SparkConf, SparkContext}


object CacheApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TopNApp")
    val sc=new SparkContext(sparkConf)
    val data=sc.textFile("hadoop-train/baidu.log")
    data.cache()
    data.collect()
    Thread.sleep(100000)
  }
}
