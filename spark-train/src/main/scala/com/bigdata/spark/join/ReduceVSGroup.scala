package com.bigdata.spark.join

import org.apache.spark.{SparkConf, SparkContext}


object ReduceVSGroup {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf()
      .setAppName("ReduceVSGroup").setMaster("local[2]")
    val a=List(1)

    val sc= new SparkContext(sparkConf)
    val data=sc.parallelize(Array("a","b","c","d","e","a","b","e","b","f"))
    val reduceData=data.map((_,1)).reduceByKey(_+_).collect()
    val groupByData=data.map((_,1)).groupByKey().map(x=>{
      (x._1,x._2.sum)
    })
  }
}
