package com.bigdata.spark.etl

import com.bigdata.spark.util.DateUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */
object LogETLApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("LogETLApp")
    val sc=new SparkContext(sparkConf)
    val data=sc.textFile("spark-train/input/baidu.log")
    
    val logs=data.map(_.split("\t")).filter(_.length==8).map(x=>{
        val cdn = x(0)
        val region = x(1)
        val level = x(2)
        val timestr = x(3)
        //var time = timestr.substring(1,timestr.length()-1);
        val time = DateUtil.parse(timestr)
        val ip = x(4)
        val domain = x(5)
        val url = x(6)
        val traffic = x(7)
        val stringBuilder = new StringBuilder("")
        stringBuilder.append(cdn).append("\t")
          .append(region).append("\t")
          .append(level).append("\t")
          .append(time).append("\t")
          .append(ip).append("\t")
          .append(domain).append("\t")
          .append(url).append("\t")
          .append(traffic);
        val result=stringBuilder.toString();
        result
    }).saveAsTextFile("spark-train/output/d=20190513")
    sc.stop()
  }
}
