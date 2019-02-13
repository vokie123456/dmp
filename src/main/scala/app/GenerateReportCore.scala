package app

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object GenerateReportCore {

  def main(args: Array[String]): Unit = {
    // 模拟企业级编程 首先判断目录是否为空
    if(args.length!=2){
      println("目录不正确，退出程序")
      sys.exit()
    }

    // 创建一个集合存储输入输出目录
    val Array(inputPath,outputPath) = args

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.parquet(inputPath)
    val logRDD = df.rdd.cache()


    // 统计各省市数据量分布情况
//    calculateProvinceCityCountBySparkCore(spark,logRDD,outputPath)

    // 计算地域分布
    calculateLocationCountBySparkCore(spark,logRDD)


  }

  /**
    * 使用sparkcore计算地域分布
    */
  def calculateLocationCountBySparkCore(spark: SparkSession, logRDD:RDD[Row]): Unit = {

  }



  /**
    * 使用sparkcore来计算各省市数据量分布情况
    * @param logRDD
    * @param outputPath
    */
  def calculateProvinceCityCountBySparkCore(spark: SparkSession,logRDD: RDD[Row], outputPath: String): Unit = {
    val countRDD: RDD[Row] = logRDD.map(line => {
      val provincename = line(24)
      val cityname = line(25)
      (provincename + "_" + cityname, 1)
    }).reduceByKey(_ + _)
      .map(tup => {
        val splited = tup._1.split("_")
        val provincename = splited(0)
        val cityname = splited(1)
        val count = tup._2
        Row(count,provincename,cityname)
      })

    val schema = StructType(
      Array(
        StructField("ct",IntegerType,true),
        StructField("provincename",StringType,true),
        StructField("cityname",StringType,true)
      )
    )

    val df = spark.createDataFrame(countRDD,schema)
    // 将数据写到mysql中
    // 创建properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456abcd")
    val url = "jdbc:mysql://192.168.58.145:3306/dmp?useUnicode=true&characterEncoding=utf8"
    df.write.jdbc(url,"provinceCityCountCore",prop)

  }




}
