package com.dahua.analyse

import com.dahua.bean.LogBean
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProCityAnalyse {

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

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[*]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._

    // 接收参数
    var Array(inputPath, outputPath) = args

    val df: DataFrame = spark.read.parquet(inputPath)

  df.createTempView("log")

    //编写sql语句
    var sql ="select provincename,cityname,count(*) as ct from log group by provincename,cityname order by ct"

    val resDF: DataFrame = spark.sql(sql)

    val configuration: Configuration = sc.hadoopConfiguration
    //文件系统对象  可以是hdfs 可以是windows本地的
    val fs: FileSystem = FileSystem.get(configuration)

    var path =new Path(outputPath)

    if(fs.exists(path)){
      fs.delete(path,true)//如果存在文件就删除
    }

    //coalesce(1) 强行弄到一个文件里面
   // resDF.coalesce(1).write.partitionBy("provincename","cityname")json(outputPath)
    resDF.coalesce(1).write.json(outputPath)

    spark.stop()
    sc.stop()

  }

}
