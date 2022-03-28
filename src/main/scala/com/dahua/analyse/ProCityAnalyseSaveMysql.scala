package com.dahua.analyse

import com.dahua.bean.LogBean
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object ProCityAnalyseSaveMysql {

/*
把数据写道mysql
 */
  def main(args: Array[String]): Unit = {
    // 判断参数是否正确。
    if (args.length != 1) {
      println(
        """
          |缺少参数
          |inputpath
          |""".stripMargin)
      sys.exit()
    }

    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 将自定义对象进行kryo序列化
    conf.registerKryoClasses(Array(classOf[LogBean]))

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[*]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._

    // 接收参数
    var Array(inputPath) = args

    val df: DataFrame = spark.read.parquet(inputPath)

    df.createTempView("log")

    //编写sql语句
    var sql ="select provincename,cityname,count(*) as ct from log group by provincename,cityname"

    val resDF: DataFrame = spark.sql(sql)

    //输出到mysql
    val load: Config = ConfigFactory.load()
    val properties =new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("driver",load.getString("jdbc.driver"))
    properties.setProperty("password",load.getString("jdbc.password"))



    resDF.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),properties)

    //关闭对象
    spark.stop()


  }

}
