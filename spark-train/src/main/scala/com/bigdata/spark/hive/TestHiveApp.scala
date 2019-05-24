package com.bigdata.spark.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scalikejdbc._
import scalikejdbc.config.DBs

import scala.collection.mutable.ListBuffer

/**
  * @Author 种庆凯
  * @Date ${date} ${time}
  */
object TestHiveApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("TestHiveApp")
    val spark = SparkSession.builder().config(sparkConf)
        .enableHiveSupport().getOrCreate()
    val dept=spark.read.table("test_dept")
    dept.foreachPartition(part=>{
      val list = new ListBuffer[Dept]
      DBs.setup('mysql)
      part.foreach(info=>{
        list.append(Dept(info.getAs("id"),info.getAs("name")))
      })
      batchSave(list.toList)
      DBs.close('mysql)
    })

  }
  def batchSave(depts:List[Dept]) :Unit= {
    NamedDB('mysql).localTx { implicit session =>
      for (dept <- depts) {
        println("==============")
        SQL("insert into dept(id,name) values(?,?)").bind(dept.id, dept.name).update().apply()
      }
    }
  }
//    def createTable(depts:List[Dept]) :Unit= {
//      NamedDB('mysql).localTx { implicit session =>
//        for (dept<- depts){
//          println("==============")
//          SQL("insert into dept(id,name) values(?,?)").bind(dept.id,dept.name).update().apply()
//        }
//      }
//  }
//  def createTable()={
//    NamedDB('mysql) autoCommit { implicit session =>
//      sql"DROP TABLE IF EXISTS `USER`".execute.apply()
//      //sql"create table user (id int primary key, name varchar(30),age int )".execute.apply()
//    }
//  }


}
case class Dept (id:String,name:String)