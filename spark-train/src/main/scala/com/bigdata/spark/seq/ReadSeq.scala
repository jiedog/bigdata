package com.bigdata.spark.seq

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author 种庆凯
  * @Date ${date} ${time}
  */
object ReadSeq {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ReadSeqApp")
    val sc=new SparkContext(sparkConf)
    //val file=sc.textFile("hadoop-train/output/d=20190324")
//    val file=sc.sequenceFile[Null,String]("hadoop-train/output/d=20190324")
//    file//collect()//.foreach(println(_))
//    file.map(x=>(x._2)).collect()
//      .foreach(println(_))
    Thread.sleep(100000)
    sc.stop()
  }
}
