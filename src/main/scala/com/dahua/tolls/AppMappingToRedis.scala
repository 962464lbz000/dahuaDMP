package com.dahua.tolls

import com.dahua.bean.LogBean
import com.dahua.utils.RedisUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object AppMappingToRedis {

  def main(args: Array[String]): Unit = {
    // 判断参数是否正确。
    if (args.length != 1) {
      println(
        """
          |缺少参数
          |inputpath  outputpath
          |""".stripMargin)
      sys.exit()
    }

    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 将自定义对象进行kryo序列化
    conf.registerKryoClasses(Array(classOf[LogBean]))

    val spark = SparkSession.builder().config(conf).appName("ZoneDimForRDD").master("local[*]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._

    // 接收参数
  var Array(inputPath) =args

    sc.textFile(inputPath).map(line =>{
      val str: Array[String] = line.split("[:]", -1)
      (str(0),str(1))
    }).foreachPartition(ite=>{

      val jedis: Jedis = RedisUtil.getJedis

      ite.foreach(mapping=>{
        jedis.set(mapping._1,mapping._2)
      })

      jedis.close()
    })

  }

}
