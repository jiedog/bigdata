package com.bigdata.scala.scalike.models


import scalikejdbc._
/**
  */
object User extends SQLSyntaxSupport[User]{
  override val tableName = "user"
  def apply(rs: WrappedResultSet) = new User(
    rs.int("id"), rs.string("name"), rs.int("age"))

}

case class User(id:Int,name:String,age:Int){
  def createUserTable(): Unit ={
    NamedDB('mysql) autoCommit { implicit session =>
      //sql"DROP TABLE IF EXISTS `USER`".execute.apply()
      sql"create table user (id int primary key, name varchar(30),age int )".execute.apply()
    }
  }
  //list返回匹配的多行记录
  def findAll(): List[User] ={
    val u = User.syntax("u")
    val users = NamedDB('mysql) readOnly{implicit session =>
      sql"select * from user".map(rs => User(rs)).list.apply()
      //withSQL { select(u.resultAll).from(User as u) }.map(rs=>User(rs)).list.apply()
    }
    users
  }
  //single返回匹配的单行作为Option值。如果意返回多行，则将抛出运行时异常。
  def findByUserId(id:Int): Option[User] ={
    val user = NamedDB('mysql) readOnly{implicit session =>
      sql"select * from user where id =${id}".map(rs => User(rs)).single().apply()
    }
    user
  }
  def insertUser(userinfo :User): Int ={
    NamedDB('mysql) localTx {implicit session =>
      sql"insert into user (id, name, age) values (${userinfo.id}, ${userinfo.name},${userinfo.age})"
        .update.apply()
    }

  }
  def insertUserBatch(): Unit ={
    NamedDB('mysql) localTx { implicit session =>
      val batchParams: Seq[Seq[Any]] = (1 to 10).map(i => Seq(i, "name" + i,i))
      sql"insert into user (id, name,age) values (?, ?,?)"
        .batch(batchParams:_*)
        .apply()
    }
  }
  def  updateAgeById(id:Int,age:Int):Int= {
    NamedDB('mysql) localTx { implicit session =>
      sql"update  user  set age =  ${age} where  id =${id}"
        .update.apply()
    }
  }
}
