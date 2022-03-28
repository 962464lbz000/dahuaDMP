package com.dahua.tag

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row


/*
渠道类型标签
 */
object AppTags extends TagTrait {
  override def makeTags(args: Any*): Map[String, Int] = {
     //设定返回值的类型
    var map =Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]

    //接收广播变量的值
    val broadcast: Map[String, String] = args(1).asInstanceOf[Map[String, String]]

    val appName: String = row.getAs[String]("appname")
    val appId: String = row.getAs[String]("appid")

    //渠道标签
    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")

    if (StringUtils.isEmpty(appName)) {
      broadcast.contains("appId") match {
        case true => map += "APP" + broadcast.getOrElse("appId", "未知") -> 1
      }
    } else {
      map += "APP" + appName -> 1
    }


    // 渠道标签
    map+= "CN"+adplatformproviderid -> 1


map
  }
}
