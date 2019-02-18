package report

import java.util.Properties

import accumulator.LogAccumulator
import constant.Constants
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import util.{NumberUtils, StringUtils}
import utils.{FilterAndAccumulatorUtils, JDBC, SchemaUtils}

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

    val logAccumulator = new LogAccumulator
    spark.sparkContext.register(logAccumulator)


    // 统计各省市数据量分布情况
//    calculateProvinceCityCountBySparkCore(spark,logRDD,outputPath)

    // 计算地域分布
    calculateLocationCountBySparkCore(spark,logRDD,logAccumulator)


    spark.stop()
  }


  /**
    * 使用sparkcore计算地域分布
    */
  def calculateLocationCountBySparkCore(spark: SparkSession, logRDD:RDD[Row],logAccumulator: LogAccumulator): Unit = {
    // 将提取logRDD中有用的字段
    val groupedRDD: RDD[(String, Iterable[String])] = logRDD.map(line => {
      val provincename = line.getString(24)
      val cityname = line.getString(25)
      val requestmode = line.getInt(8)
      val processnode = line.getInt(35)
      val iseffective = line.getInt(30)
      val isbilling = line.getInt(31)
      val isbid = line.getInt(39)
      val iswin = line.getInt(42)
      val adorderid = line.getInt(2)
      val winprice = line.getDouble(41)
      val adpayment = line.getDouble(75)

      // 拼接字段
      val provinceCityName = provincename + "_" + cityname
      val filterCondition = Constants.REQUESTMODE+"="+requestmode+"|"+Constants.PROCESSNODE + "=" + processnode + "|" +
        Constants.ISEFFECTIVE + "=" + iseffective + "|" + Constants.ISBILLING + "=" + isbilling + "|" +
        Constants.ISBID + "=" + isbid + "|" + Constants.ISWIN + "=" + iswin + "|" + Constants.ADORDEERID + "=" + adorderid+ "|"+
        Constants.WINPRICE+"="+winprice+"|"+ Constants.ADPAYMENT+"="+adpayment

      (provinceCityName, filterCondition)

    }).groupByKey

    val accumulatorRDD = FilterAndAccumulatorUtils.filterAndAccumulator(groupedRDD,logAccumulator)

    // 将累加后的字段拆分出来,并使用Row进行封装
    val provinceCityRowRDD: RDD[Row] = accumulatorRDD.map(tup => {
      val provinceCityname = tup._1.split("_")
      val provincename = provinceCityname(0)
      val cityname = provinceCityname(1)

      val aggrInfo = tup._2
      val original_request_count = (StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.ORIGINAL_REQUEST_COUNT)).toInt
      val effective_request_count = (StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.EFFECTIVE_REQUEST_COUNT)).toInt
      val ad_request_count = (StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.AD_REQUEST_COUNT)).toInt
      val join_biding_count = (StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.JOIN_BIDING_COUNT)).toInt
      val biding_win_count = (StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.BIDING_WIN_COUNT)).toInt
      val biding_win_rate = NumberUtils.formatDouble(biding_win_count.toDouble / join_biding_count.toDouble, 2)
      val show_count = (StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.SHOW_COUNT)).toInt
      val click_count = (StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.CLICK_COUNT)).toInt
      val click_rate = NumberUtils.formatDouble(click_count.toDouble / show_count.toDouble, 2)
      val DSPwinprice = NumberUtils.formatDouble((StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.DSPWINPRICE)).toDouble / 1000, 2)
      val DSPadpayment = NumberUtils.formatDouble((StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.DSPADPAYMENT)).toDouble / 1000, 2)

      Row(provincename, cityname, original_request_count, effective_request_count, ad_request_count, join_biding_count, biding_win_count,
        biding_win_rate, show_count, click_count, click_rate, DSPwinprice, DSPadpayment)
    })

    val provinceCityCoreDF: DataFrame = spark.createDataFrame(provinceCityRowRDD,SchemaUtils.provinceCitySchema)
    JDBC.createTable("requestcore",provinceCityCoreDF)
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
