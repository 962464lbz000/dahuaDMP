package com.dahua.analyse
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}

import scala.collection.mutable


object ProCityAnalyseForRDD1 {

    def main(args: Array[String]): Unit = {

      // 判断参数是否正确。
      if (args.length != 2) {
        println(
          """
            |缺少参数
            |inputpath outputpath
            |""".stripMargin)
        sys.exit()
      }

      // 创建sparksession对象
      var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      val spark = SparkSession.builder().config(conf).appName("Log2Parquet").getOrCreate()


      var sc = spark.sparkContext

      var Array(inputPath, outputPath) = args

      val line: RDD[String] = sc.textFile(inputPath)
      val filed: RDD[Array[String]] = line.map(_.split(",", -1))
      val procityRDD: RDD[((String, String), Int)] = filed.filter(_.length >= 85).map(arr => {
        var pro = arr(24)
        var city = arr(25)
        ((pro, city), 1)
      })
      val reduceRDD: RDD[((String, String), Int)] = procityRDD.reduceByKey(_ + _)


      val num: Int = reduceRDD.map(
        _._1._1
      ).distinct().count().toInt


      reduceRDD.sortBy(_._2).coalesce(1).partitionBy(new MyPartition(num)).saveAsTextFile(outputPath)

      spark.stop()
      sc.stop()
    }
  }

  class MyPartition(val count: Int) extends Partitioner {
    override def numPartitions: Int = count

    private var num = -1

    private val map: mutable.Map[String, Int] = mutable.Map[String, Int]()

    override def getPartition(key: Any): Int = {
      val value: String = key.toString
      val str: String = value.substring(1, value.indexOf(","))
      println(str)

      if (map.contains(str)) {
        map.getOrElse(str, num)
      } else {
        num += 1
        map.put(str, num)
        num
      }
    }

}