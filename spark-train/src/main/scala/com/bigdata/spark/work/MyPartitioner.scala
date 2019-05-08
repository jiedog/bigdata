package com.bigdata.spark.work

import org.apache.spark.{Partitioner, SparkConf, SparkContext}


object MyPartitioner {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("partitioner").setMaster("local")
    val sc=new SparkContext(conf)

    //val data=sc.parallelize(List("aa.2","bb.2","cc.3","dd.3","ee.5"))
    val data=sc.parallelize(List(1,2,3,4,5,6,7,8),24)
//    data.foreachPartition(x=>{
//      println(x)
//    })
    //println( data.getNumPartitions)
    val logFile = sc.textFile("hadoop-train/baidu.log")
    logFile.glom().foreach(x=>{
      println(x)
    })

   //sc.hadoopFile("")

  //  println(logFile.getNumPartitions)
   // data.reduce(_-_).toInt
//    data
//    println(data.reduce(_-_))
//    data.map((_,1)).repartitionAndSortWithinPartitions(new MyPartitioner(2))
//      .foreach(x=>{
//        println("===="+x+"=====")
//        x
//      })
//      .foreachPartition(println(_))
    Thread.sleep(10000000)
    sc.stop()
  }
}
case class MyPartitioner(numParts:Int) extends Partitioner{
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int =  {
    val ckey: String = key.toString
    ckey.substring(ckey.length-1).toInt%numPartitions
  }
}
