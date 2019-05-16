package com.bigdata.spark.etl

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}


object TopNApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TopNApp")
    val sc=new SparkContext(sparkConf)
    val data=sc.textFile("spark-train/output/d=20190513")

    val top2=data.map(x=>{
      val loginfo=x.split("\t")
      val length = loginfo.length
      var traffic=0l
      var domain=""
      if(length==8){
        domain=loginfo(6)
        try{
          traffic=loginfo(7).toLong
        }catch {
          case e:Exception=>e.printStackTrace()
        }

      }
      (domain,traffic)
    }).reduceByKey(_+_).top(2)

    sc.parallelize(top2).coalesce(1)
          .map(x=>{x._1+"\t"+x._2})
        .re

        .saveAsTextFile("spark-train/output/result/d=20190513")

  }
}

case class MyPartitioner(partitions: Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    Math.abs(key.hashCode()%numPartitions)
  }
}
