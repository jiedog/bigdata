package com.bigdata.spark.etl


import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */
object LogETLApp {
  def main(args: Array[String]): Unit = {
//    val sparkConf = new SparkConf()
//      //.setMaster("local[*]")
//      .setAppName("LogETLApp")
//      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//       // .set("spark.kryo.register","com.bigdata.spark.etl.MyRegis")
//    sparkConf.registerKryoClasses(Array(classOf[EtlApp ]))
//
//    val sc=new SparkContext(sparkConf)
//    val data=sc.textFile("/hadoop/baidu.log")
   //val data=sc.textFile("hadoop-train/baidu.log")
    //val data=sc.textFile("spark-train/input/baidu.log")
    //println(data.count())
    //data.persist(StorageLevel.MEMORY_ONLY_SER)
 //   data.cache()
//    var  y = 1
//    val ss=sc.longAccumulator("aa")
//    data.map(x=>{
//      y=y+1
//      ss.add(1)
//      (x,1)
//    }).take(10)

 //   println(y)
  //  println(ss)
    //Thread.sleep(100000l)
//    val logs=data.map(_.split("\t")).filter(_.length==8).map(x=>{
//       //new etlAPP().etl(x)
//      //val a = new EtlApp()
//    EtlApp.etl(x)
//    })//.foreach(println(_))
//        .take(10).foreach(println(_))
//      //.saveAsTextFile("/hadoop/coaoutputsnapppy",classOf[SnappyCodec])
//    sc.stop()
  }

}

