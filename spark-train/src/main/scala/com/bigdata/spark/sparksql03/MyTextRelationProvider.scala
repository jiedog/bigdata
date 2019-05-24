package com.bigdata.spark.sparksql03

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
  * @Author 种庆凯
  * @Date ${date} ${time}
  */
class MyTextRelationProvider(override val sqlContext : SQLContext, path : String, userSchema : StructType)  extends BaseRelation
  with TableScan
  //with PrunedScan
  with PrunedFilteredScan
  with Serializable {

  override def schema: StructType = {
      println("schema : schema is called")
      if (userSchema != null) {
        userSchema
      } else {
        StructType(
                  StructField("cdn",StringType,nullable = true)::
                  StructField("region",StringType,nullable = true)::
                  StructField("level",StringType,nullable = true)::
                  StructField("time",StringType,nullable = true)::
                  StructField("ip",StringType,nullable = true)::
                  StructField("domain",StringType,nullable = true)::
                  StructField("url",StringType,nullable = true)::
                  StructField("traffic",StringType,nullable = true)::Nil
        )
      }

  }

  override def buildScan(): RDD[Row] ={
    println("TableScan: buildScan called...")
    val schemaFileds = schema.fields
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(_._2)
    val rows = rdd.map(x=>{
      val lines = x.split("\n")
      val data=lines.map(line=>{
        line.split("\t").map(col=>{
          col.trim
        }).toSeq
      })
      data.foreach(println(_))
      val tmp = data.filter(_.length==8)
        .map(cols => {

        cols.zipWithIndex.map({
          case (value,index)=>{
            val colname=schemaFileds(index).name
            if(colname=="time"){
              val sourceFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z", Locale.ENGLISH)
              val targetFormat = new SimpleDateFormat("yyyyMMddHHmmss")
              val time=targetFormat.format(sourceFormat.parse(value.substring(1,value.length-1)))
              time
            }else{
              value
            }

          }
        })
      })

      tmp.map(y=>{
        Row.fromSeq(y)
      })
    })
    rows.flatMap(z=>z)
  }

//  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
//    println("TableScan: buildScan called..."+requiredColumns.toString)
//    val schemaFileds = schema.fields
//    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(_._2)
//    val rows = rdd.map(x=>{
//      val lines = x.split("\n")
//      val data=lines.map(line=>{
//        line.split("\t").map(col=>{
//          col.trim
//        }).toSeq
//      })
//      data.foreach(println(_))
//      val tmp = data.filter(_.length==8)
//        .map(cols => {
//
//          cols.zipWithIndex.map({
//            case (value,index)=>{
//              val colname=schemaFileds(index).name
//              val colValue=if(colname=="time"){
//                val sourceFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z", Locale.ENGLISH)
//                val targetFormat = new SimpleDateFormat("yyyyMMddHHmmss")
//                val time=targetFormat.format(sourceFormat.parse(value.substring(1,value.length-1)))
//                time
//              }else{
//                value
//              }
//              println(colValue)
//              if(requiredColumns.contains(colname)){
//                Some(colValue)
//              }else{
//                None
//              }
//
//            }
//          })
//        })
//
//      tmp.map(y=>{
//        Row.fromSeq(y.filter(!_.isEmpty).map(_.get))
//      })
//    })
//    rows.flatMap(z=>z)
//  }
//
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    println("TableScan: buildScan called..."+requiredColumns.toString)
    val schemaFileds = schema.fields
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(_._2)
    val rows = rdd.map(x=>{
      val lines = x.split("\n")
      val data=lines.map(line=>{
        line.split("\t").map(col=>{
          col.trim
        }).toSeq
      })
     // data.foreach(println(_))
      val tmp = data.filter(_.length==8)
        .map(cols => {

          cols.zipWithIndex.map({
            case (value,index)=>{
              val colname=schemaFileds(index).name
              val colValue=if(colname=="time"){
                val sourceFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z", Locale.ENGLISH)
                val targetFormat = new SimpleDateFormat("yyyyMMddHHmmss")
                val time=targetFormat.format(sourceFormat.parse(value.substring(1,value.length-1)))
                time
              }else{
                value
              }
              println(colValue)
              if(requiredColumns.contains(colname)){
                Some(colValue)
              }else{
                None
              }

            }
          })
        })

      tmp.map(y=>{
        Row.fromSeq(y.filter(!_.isEmpty).map(_.get))
      })
    })
    rows.flatMap(z=>z)
  }
}
