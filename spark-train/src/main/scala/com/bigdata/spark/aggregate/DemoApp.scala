package com.bigdata.spark.aggregate

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author 种庆凯
  * @Date ${date} ${time}
  */
object DemoApp {
  def main(args: Array[String]): Unit = {
//
    val str= "http://v1.com/c?a=b"
    val str2=str.split("\\?")
    println(str2)
   // rdd3.aggregate("")((x,y) => math.max(x.length, y.length).toString, (x,y) => x + y)


  }
}
