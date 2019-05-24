package com.bigdata.spark.sparksql03

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/**
  * @Author 种庆凯
  * @Date ${date} ${time}
  */
class DefaultSource extends RelationProvider
  with SchemaRelationProvider
  with DataSourceRegister
  {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext,parameters,null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val path = parameters.get("path")
    path match {
      case Some(p) => new MyTextRelationProvider(sqlContext, p, schema)
      case _ => throw new IllegalArgumentException("Path is required for custom-datasource format!!")
    }
  }

    override def shortName(): String = ???
  }
