package com.dahua.tag

import com.dahua.tolls.SNTools
import org.apache.spark.sql.Row

object BCTags extends TagTrait {
  override def makeTags(args: Any*): Map[String, Int] = {
    var map =Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]

    //经度
    val longs: String = row.getAs[String]("long")

    //纬度
    val lat: String = row.getAs[String]("lat")

    val str: String = SNTools.getBusiness(lat + "," + longs)

    if(str != "" ){
      println("纬度："+lat+"经度："+longs)
      println("商圈："+str)

      map += "SN"+str -> 1
    }
    map
  }
}
