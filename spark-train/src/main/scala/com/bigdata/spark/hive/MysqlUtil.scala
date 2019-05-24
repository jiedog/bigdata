package com.bigdata.spark.hive

import java.sql.DriverManager

import com.mysql.jdbc.{Connection, PreparedStatement}

/**
  * @Author 种庆凯
  * @Date ${date} ${time}
  */
object MysqlUtil {


    /**
      * 获取数据库连接
      */
    def getConnection() = {
      DriverManager.getConnection("jdbc:mysql://localmysql:3306/bigdata?user=root&password=root&useUnicode=true&characterEncoding=utf-8&useSSL=false")
    }

    /**
      * 释放数据库连接等资源
      * @param connection
      * @param pstmt
      */
    def release(connection: Connection, pstmt: PreparedStatement): Unit = {
      try {
        if (pstmt != null) {
          pstmt.close()
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (connection != null) {
          connection.close()
        }
      }
    }

    def main(args: Array[String]) {
      println(getConnection())
    }



}
