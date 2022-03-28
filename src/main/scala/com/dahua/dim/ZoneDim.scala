package com.dahua.dim


import com.dahua.bean.LogBean
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object ZoneDim {

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

    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 将自定义对象进行kryo序列化
    conf.registerKryoClasses(Array(classOf[LogBean]))

    val spark = SparkSession.builder().config(conf).appName("ZoneDim").master("local[*]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._

    // 接收参数
    var Array(inputPath, outputPath) = args

    val df: DataFrame = spark.read.parquet(inputPath)

    //创建表
    df.createTempView("dim")

    var sql =
      """
        |select
        |provincename,cityname,
        |sum(case when requestmode =1 and processnode >=1 then 1 else 0 end )as ysqq,
        |sum(case when requestmode =1 and processnode >=2 then 1 else 0 end )as yxqq,
        |sum(case when requestmode =1 and processnode = 3 then 1 else 0 end )as ggqq,
        |sum(case when adplatformproviderid >=100000 and iseffective =1 and isbilling = 1 and isbid =1 and adorderid != 0 then 1 else 0 end )as cyjjs,
        |sum(case when adplatformproviderid >=100000 and iseffective =1 and isbilling = 1 and iswin =1  then 1 else 0 end )as jjcgs,
        |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )as zss,
        |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )as djs,
        |sum(case when requestmode =2 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjzss,
        |sum(case when requestmode =3 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjdjs,
        |sum(case when adplatformproviderid >=100000 and iseffective =1 and isbilling = 1 and iswin =1  then (winprice*1.0)/1000 else 0 end )as xiaofei,
        |sum(case when adplatformproviderid >=100000 and iseffective =1 and isbilling = 1 and iswin =1  then (adpayment*1.0)/1000 else 0 end )as chengben
        |from dim
        |group by
        |provincename ,cityname
        |""".stripMargin


    val frame: DataFrame = spark.sql(sql)

    val config: Config = ConfigFactory.load()

    val load: Config = ConfigFactory.load()
    val properties =new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("driver",load.getString("jdbc.driver"))
    properties.setProperty("password",load.getString("jdbc.password"))



    frame.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName1"),properties)



    spark.stop()
    sc.stop()




  }

}
