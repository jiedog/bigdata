package com.bigdata.spark.share

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广播变量
  */
object BroadcastApp {
  def main(args: Array[String]): Unit = {


    val sparkConf=new SparkConf()
      .setAppName("BroadcastApp").setMaster("local[2]")
    val sc= new SparkContext(sparkConf)
    val rdd1=sc.parallelize(List((1,"a"),(2,"b"),(1,"c"),(2,"d"),(3,"e")))
    val data=sc.parallelize(List((1,"AA"),(2,"BB")))
    //val broadcastdata=sc.broadcast(data)
    //java.lang.IllegalArgumentException: requirement failed: Can not directly broadcast RDDs; instead, call collect() and broadcast the result.
    //不能直接对RDD进行广播需要collect获取
    //修改为
    val broadcastdata=sc.broadcast(data.collectAsMap())
    //broadcastdata.value.foreach(println(_))//1 2 3 4 5
    //rdd1.join(data).foreach(println(_))
    //data.join(rdd1).foreach(println(_))
    rdd1.mapPartitions(x=>{
      val mapdata=broadcastdata.value
      for((key,value) <- x if(mapdata.contains(key)))
        yield (key,mapdata.get(key),value)
    }).foreach(println(_))
    rdd1.mapPartitions(x=>{
      val mapdata=broadcastdata.value
      for((key,value) <- x if(mapdata.contains(key)))
        yield (key,mapdata.get(key).getOrElse(""),value)
    }).foreach(println(_))
    Thread.sleep(1000000)
  }

}
