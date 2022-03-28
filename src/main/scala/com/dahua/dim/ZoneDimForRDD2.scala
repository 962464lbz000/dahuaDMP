package com.dahua.dim

import com.dahua.bean.LogBean
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ZoneDimForRDD2 {

  def main(args: Array[String]): Unit = {

    // 判断参数是否正确。
    if (args.length != 3) {
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
    var Array(inputPath,appMapping,outputPath) = args

    //读取映射文件  appMapping
    val map: Map[String, String] = sc.textFile(appMapping).map(line => {
      val arr: Array[String] = line.split("[:]", -1)
      (arr(0), arr(1))
    }).collect().toMap

    //使用广播变量  进行广播
    val appBroadcast: Broadcast[Map[String, String]] = sc.broadcast(map)

    val log: RDD[String] = sc.textFile(inputPath)

    val logRDD: RDD[LogBean] = log.map(_.split(",", -1)).filter(_.length >= 85).map(LogBean(_)).filter(t => {
      //表示不能为空的
      !t.appid.isEmpty
    })

    logRDD.map(log =>{
      val appname: String = log.appname
     if(appname == "" || appname.isEmpty){
       val appname: String = appBroadcast.value.getOrElse(log.appid, "不明确")
     }

      val ysqqs: List[Double] = DIMZhibiao.ysqqsRtp(log.requestmode, log.processnode)

      (appname,ysqqs)

    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).foreach(println)


  }
}
