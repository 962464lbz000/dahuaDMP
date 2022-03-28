package com.dahua.tag

import com.dahua.utils.TagUtil
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.util.UUID

object TagRpt {


  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println(
        """
          |缺少参数
          |inputpath,appMapping,stopwords  outputpath
          |""".stripMargin)
      sys.exit()
    }

    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._

    // 接收参数
    var Array(inputPath,app_Mapping,stopWords, outputPath) = args

    // 读取app_Mapping广播变量.
    val app_map: Map[String, String] = sc.textFile(app_Mapping).map(line => {
      val strs: Array[String] = line.split("[:]", -1)
      (strs(0), strs(1))
    }).collect().toMap

    // 广播app变量
    val broadCastAppMap: Broadcast[Map[String, String]] = sc.broadcast(app_map)

    // 停用词广播变量.
    val stopWordMap: Map[String, Int] = sc.textFile(stopWords).map((_, 0)).collect().toMap
    val broadcastStopWord: Broadcast[Map[String, Int]] = sc.broadcast(stopWordMap)


    // 读取数据源,打数据标签.
    val df: DataFrame = spark.read.parquet(inputPath)

    val TagDS: Dataset[(String, List[(String, Int)])] = df.where(TagUtil.tagUserIdFilterParam).map(row => {
      // 广告标签
      val adsMap: Map[String, Int] = AdsTags.makeTags(row)
      // app标签.
      val appMap: Map[String, Int] = AppTags.makeTags(row, broadCastAppMap.value)
      // 驱动标签
      val driverMap: Map[String, Int] = DriverTags.makeTags(row)
      // 关键字标签
      val keyMap: Map[String, Int] = KeyTags.makeTags(row, broadcastStopWord.value)
      // 地域标签
      val pcMap: Map[String, Int] = PCTags.makeTags(row)
      // 商圈标签.
      val bcMap: Map[String, Int] = BCTags.makeTags(row)




      // 获取用户ID
      //      (TagUtil.getUserId(row)(0),(adsMap++appMap++driverMap++keyMap++pcMap).toList)
      if (TagUtil.getUserId(row).size > 0) {
        (TagUtil.getUserId(row)(0), (adsMap ++ appMap ++ driverMap ++ keyMap ++ pcMap ++ bcMap).toList)
      } else {
        (UUID.randomUUID().toString.substring(0, 6), (adsMap ++ appMap ++ driverMap ++ keyMap ++ pcMap ++ bcMap).toList)
      }
    })
    TagDS.rdd.reduceByKey((list1,list2)=>{
      (list1++list2).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList
    }).saveAsTextFile(outputPath)





  }



}
