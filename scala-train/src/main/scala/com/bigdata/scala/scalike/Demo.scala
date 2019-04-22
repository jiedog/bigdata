package com.bigdata.scala.scalike

import com.bigdata.scala.scalike.models.User

/**
  *
  */
object Demo {
  def main(args: Array[String]): Unit = {
    import scalikejdbc._
    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton("jdbc:mysql://localmysql:3306/bigdata?useUnicode=true&characterEncoding=utf-8&useSSL=false", "root", "root")

    // ad-hoc session provider on the REPL
    implicit val session = AutoSession



    sql"""insert into user (id, name, age) values (2, "bbb", 2)"""
      .update.apply()
    sql"update user set id = 8 where id = 8".update.apply()
    val users: List[User] = sql"select * from user".map(rs => User(rs)).list.apply()
    users.foreach(x=>{
      println(x)
    })
  }
}


