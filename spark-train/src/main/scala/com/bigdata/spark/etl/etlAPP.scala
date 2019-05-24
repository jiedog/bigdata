package com.bigdata.spark.etl

/**
  * @Author 种庆凯
  * @Date ${date} ${time}
  */
class etlAPP {
  def etl(x:Array[String]): String ={
    val cdn = x(0)
    val region = x(1)
    val level = x(2)
    val time = x(3)
    //var time = timestr.substring(1,timestr.length()-1);
    //val time = DateUtil.parse(timestr)
    val ip = x(4)
    val domain = x(5)
    val url = x(6)
    val traffic = x(7)
    val stringBuilder = new StringBuilder("")
    stringBuilder.append(cdn).append("\t")
      .append(region).append("\t")
      .append(level).append("\t")
      .append(time).append("\t")
      .append(ip).append("\t")
      .append(domain).append("\t")
      .append(url).append("\t")
      .append(traffic);
    val result=stringBuilder.toString();
    result
  }
}
