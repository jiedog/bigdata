package com.bigdata.spark.work

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */
object DomainTopApp2 {
  def main(args: Array[String]): Unit = {

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
      * 2019050510,a.com,/1.html 100  traffic
      */
    val logFile = sc.textFile("spark-train/input/baidu.log")

    //logFile.collect().foreach(println(_))
    logFile.map(x=>{
      val loginfo=x.split("\t")
      val length = loginfo.length
      var traffic=0l
      var domain=""
      var urlFile=""
      if (length==8){
        domain=loginfo(6)
        try{
          traffic=loginfo(7).toLong
        }catch {
          case e:Exception=> e.printStackTrace()
        }

        var urls=loginfo(5).split(".com")
        if(urls.length==2){
          if(urls(1).contains("?")){
            val files= urls(1).split("\\?")
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
        ((domain,urlFile),(1,traffic))
      }else{
        (("-","-"),(1,traffic))
      }
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
        .map(x=>{
          (x._1._1,(x._1._2,x._2._1,x._2._2))
        }).groupByKey()
        .mapValues(x=>{
          x.toList.sortWith(_._2>_._2).take(2).sortBy(_._2).reverse
        })
          .map(x=>{
            x._2.map(y=>{
              (x._1,y._1,y._2,y._3)
            })
          })
        .flatMap(_.map(x=>{
          (x._1,x._2,x._3,x._4)
        }))


//
//    })
      .collect().foreach(println(_))
//
//    //   .saveAsTextFile("spark-train/output/baidu")


    sc.stop()
  }

}
