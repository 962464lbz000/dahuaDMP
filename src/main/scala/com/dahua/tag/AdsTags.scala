package com.dahua.tag

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row


/*
广告位类型标签
 */
object AdsTags extends TagTrait {
  override def makeTags(args: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]

    //广告位类型
    val adspacetype: Int = row.getAs[Int]("adspacetype")

    if(adspacetype >9){
      map += "LC" + adspacetype ->1
    }else {
      map += "LC0" +adspacetype ->1
    }

    //广告位名称
    val adspacetypename: String = row.getAs[String]("adspacetypename")

    if(StringUtils.isNotEmpty(adspacetypename)){
      map += "LN" + adspacetypename -> 1
    }

map
  }
}
