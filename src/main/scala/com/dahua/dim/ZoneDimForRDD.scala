package com.dahua.dim

import com.dahua.bean.LogBean
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ZoneDimForRDD {

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

    val spark = SparkSession.builder().config(conf).appName("ZoneDimForRDD").master("local[*]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._

    // 接收参数
    var Array(inputPath, outputPath) = args

    val df: DataFrame = spark.read.parquet(inputPath)

    val dimRDD: Dataset[((String, String), List[Double])] = df.map(row => {
      //获取字段
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processNode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      val province: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")
      val appname: String = row.getAs[String]("appname")
      val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")


      //将维度写道方法里面
      val ysqqs: List[Double] = DIMZhibiao.ysqqsRtp(requestmode, processNode)
      val cyjjs: List[Double] = DIMZhibiao.jingjiaRtp(adplatformproviderid, iseffective, isbilling, isbid, iswin, adorderid)
      val ggzss: List[Double] = DIMZhibiao.ggzjRtp(requestmode, iseffective)
      val mjzss: List[Double] = DIMZhibiao.mjjRtp(requestmode, iseffective, isbilling)
      val ggxf: List[Double] = DIMZhibiao.ggcbRtp(adplatformproviderid, iseffective, isbilling, iswin, winprice, adpayment)

      ((province, cityname), ysqqs ++ cyjjs ++ ggzss ++ mjzss ++ ggxf)
    })

    //如何做聚合
    dimRDD.rdd.reduceByKey((list1,list2)=>{
      //zip:拉链函数
      list1.zip(list2).map(x=>x._1+x._2)
    }).foreach(println)




  }

}
