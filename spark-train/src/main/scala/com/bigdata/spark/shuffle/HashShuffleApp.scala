package com.bigdata.spark.shuffle

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author 种庆凯
  * @Date ${date} ${time}
  */
object HashShuffleApp {
  def main(args: Array[String]): Unit = {

//      val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TopNApp")
//      sparkConf.set("spark.shuffle.manager","hash")
//    //  sparkConf.setExecutorEnv("spark.shuffle.manager","hash")
//    sparkConf.set("spark.local.dir","G:\\block")
//      val sc=new SparkContext(sparkConf)
//      val data=sc.textFile("spark-train/input/baidu.log")
//      val a=data.map((_,1)).repartition(20).collect()

      Thread.sleep(10000000)

  }

}
