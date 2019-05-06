package com.bigdata.spark.work


import java.lang.Exception

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  *
  */
object DomainTopApp {
  def main(args: Array[String]): Unit = {
    Window.partitionBy()
    if(args.length==2){
      println("""
         |input 没值
         |output 没值
       """.stripMargin)
      System.exit(1)
    }
    val sparkConf=new SparkConf()
      .setAppName("DomainTopApp").setMaster("local[2]")
    val sc= new SparkContext(sparkConf)

    /**
      *使用Spark Core完成每个域名下访问数TOP10的文件资源
      *文件资源解释：
      *url为：http://ruozedata.com/line.html  则文件资源是/line.html
      *url为：http://ruozedata.com/a/b/c/line.html?a=b&c=d， 则文件资源是/a/b/c/line.html
      * =>(domain,url)=> redeceByKey
      *
      */
    val logFile = sc.textFile("spark-train/input/baidu.log")
    //logFile.collect().foreach(println(_))
    logFile.map(x=>{
      val loginfo=x.split("\t")
      val length = loginfo.length
      var domain=""
      var urlFile=""
      if (length==8){
        domain=loginfo(6)
        var urls=loginfo(5).split(".com")
        if(urls.length==2){
          if(urls(1).contains("?")){
            val files= urls(1).split("?")
            if(files.length==2){
              urlFile=files(0)
            }else{
              urlFile="-"
            }
          }else{
            urlFile=urls(1)
          }

        }else{
          urlFile="-"
        }
        ((domain,urlFile),1)
      }else{
        (("-","-"),1)
      }
    }).reduceByKey(_+_)
        .map(x=>{
          (x._1._1,(x._1._2,x._2))
        })
        .groupByKey()
        .mapValues(x=>{
          (x.toList.sortWith(_._2>_._2).take(2))
        })
        .map(x=>{
          x._2.map(y=>{
            (x._1,y._1,y._2)
          })
        })
        .flatMap(x=>{
          x.map(y=>{
            y
          })
        })

   .collect().foreach(println(_))

     // .saveAsTextFile("spark-train/output/baidu")


    sc.stop()
  }
  def logETL(log:String): Unit ={
    val loginfo=log.split("\t")
    val length = loginfo.length

    if(length==8){
      ( loginfo(6),urlETL(loginfo(5)))
    }else{
      ("-","-")
    }
  }
  /*url为：http://ruozedata.com/line.html  则文件资源是/line.html
  *url为：http://ruozedata.com/a/b/c/line.html?a=b&c=d， 则文件资源是/a/b/c/line.html
  */
  def urlETL(url:String): String ={
    if(url.contains("?")){

    }else{

    }
    ""
  }
  def oneToMore(): Unit ={

  }
//  def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = self.withScope {
//    // groupByKey shouldn't use map side combine because map side combine does not
//    // reduce the amount of data shuffled and requires all map side data be inserted
//    // into a hash table, leading to more objects in the old gen.
//    val createCombiner = (v: V) => CompactBuffer(v)
//    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
//    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
//    val bufs = combineByKeyWithClassTag[CompactBuffer[V]](
//      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
//    bufs.asInstanceOf[RDD[(K, Iterable[V])]]
//  }
}
