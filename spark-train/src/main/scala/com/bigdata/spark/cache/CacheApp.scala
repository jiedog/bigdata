package com.bigdata.spark.cache

import org.apache.spark.{SparkConf, SparkContext}


object CacheApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("TopNApp")
//    sparkConf.set("spark.shuffle.manager","hash")
//    sparkConf.setExecutorEnv("spark.shuffle.manager","hash")
    val sc=new SparkContext(sparkConf)
    val data=sc.textFile("hadoop-train/baidu.log")
    data.foreachPartition(x=>{
      println(x+"1")
    })
    //val a=data.map((_,1)).reduceByKey(_+_).collect()
   // val data = sc.parallelize(Array(1,2,3,4,5,6))
//  println(data.glom().collect())
//    data.glom().foreach(println(_))
    //Thread.sleep(100000)
  }
}
