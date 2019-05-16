package com.bigdata.spark.join

import org.apache.spark.{SparkConf, SparkContext}


object Repartition {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf()
      .setAppName("RepartitionVsCoalesceApp").setMaster("local[2]")
    val a=List(1)

    val sc= new SparkContext(sparkConf)
    val data=sc.parallelize(Array("a","b","c","d","e","a","b","e","b","f"),2)
    data.glom().collect()
    data.repartition(1).glom().collect()
    data.repartition(2).glom().collect()
    data.repartition(3).glom().collect()
    data.coalesce(1).glom().collect()
    data.coalesce(2).glom().collect()
    data.coalesce(3).glom().collect()
    data.coalesce(1,true).glom().collect()
    data.coalesce(2,true).glom().collect()
    data.coalesce(3,true).glom().collect()



  }


}
