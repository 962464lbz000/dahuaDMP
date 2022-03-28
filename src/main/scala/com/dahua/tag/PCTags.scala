package com.dahua.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object PCTags  extends  TagTrait {
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]
    val provinceName: String = row.getAs[String]("provincename")
    val cityName: String = row.getAs[String]("cityname")
    if(StringUtils.isNotEmpty("provinceName")) map += "ZP"+provinceName->1
    if(StringUtils.isNotEmpty("cityname")) map += "ZL"+cityName->1
    map


  }
}
