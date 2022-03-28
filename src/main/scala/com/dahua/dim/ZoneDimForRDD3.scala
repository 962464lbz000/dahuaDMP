package com.dahua.dim

import com.dahua.bean.LogBean
import com.dahua.utils.RedisUtil
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object ZoneDimForRDD3 {

  def main(args: Array[String]): Unit = {

    // 判断参数是否正确。
    if (args.length != 2) {
      println(
        """
          |缺少参数
          |inputpath  outputpath
          |""".stripMargin)
      sys.exit()
    }


    val Array(inputPath, outputPath) = args

    //创建sparkconf -> sparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName("ZoneDimForRDD3")
    sparkConf.setMaster("local[*]")

    //rdd序列化
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)


    //读取数据
    val log: RDD[String] = sc.textFile(inputPath)

    val logRDD: RDD[LogBean] = log.map(_.split(",", -1)).filter(_.length >= 85).map(LogBean(_)).filter(t => {
      //表示不能为空的
      !t.appid.isEmpty
    })

    logRDD.mapPartitions(iter => {
      //连接Redis
      val jedis: Jedis = RedisUtil.getJedis

      //自定义一个ListBuffer
      val parResult = new collection.mutable.ListBuffer[(String, List[Double])]()

      //遍历分区的所有数据，查询redis (把appname为空的数据进行转换)，将结果放入到 ListBuffer
      iter.foreach(log => {
        var appname: String = log.appname
        if (StringUtils.isEmpty(appname)) {
          appname = jedis.get(log.appid)
        }
        val ysqqs: List[Double] = DIMZhibiao.ysqqsRtp(log.requestmode, log.processnode)

        parResult += ((appname, ysqqs))

      })

      jedis.close()
      //将listBuffer转成迭代器
      parResult.toIterator

    })
      .reduceByKey((list1, list2) => {
        list1.zip(list2).map(t => t._1 + t._2)
      }).foreach(println)


    sc.stop()
  }
}