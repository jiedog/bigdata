package com.bigdata.spark.share

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author 种庆凯
  * @Date ${date} ${time}
  */
object AccumulatorsApp {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf()
      .setAppName("AccumulatorsApp").setMaster("local[2]")
    val sc= new SparkContext(sparkConf)
    val accm = sc.longAccumulator("myaccm")
    val data=sc.parallelize(Array(1,2,3,4,5))
    data.map(x=>{
      accm.add(x)
      println(accm)
      x
    }).collect()
    println(accm.value)


  }
}
