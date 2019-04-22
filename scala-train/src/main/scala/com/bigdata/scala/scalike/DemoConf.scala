package com.bigdata.scala.scalike

import com.bigdata.scala.scalike.models.User
import scalikejdbc._
import scalikejdbc.config._

/**
  *
  */
object DemoConf {


  // default
 // val conn: java.sql.Connection = ConnectionPool.borrow()
  // named
//  val conn: java.sql.Connection = ConnectionPool('mysql).borrow()
  def main(args: Array[String]): Unit = {
    //加载jkdbc驱动程序
    DBs.setup('mysql)


   val user = new User(4,"ccc",4)
   //insert(user)
  // findByUserId(2).foreach(println(_))
   //createUserTable
   user.updateAgeById(8,user.findByUserId(8).map(x=>x.age).get+10)
   user.findAll().foreach(println(_))
    //关闭数据库连接
   //insertUserBatch
    DBs.closeAll()
  }

}
