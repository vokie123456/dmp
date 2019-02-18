package report

import accumulator.LogAccumulator
import constant.Constants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import util.{NumberUtils, StringUtils}
import utils.{FilterAndAccumulatorUtils, JDBC, JedisPools, SchemaUtils}

import scala.collection.mutable.ListBuffer

object AppNameCount {
  def main(args: Array[String]): Unit = {

    // 模拟企业级编程 首先判断目录是否为空
    if(args.length!=2){
      println("目录不正确，退出程序")
      sys.exit()
    }

    // 创建一个集合存储输入输出目录
    val Array(inputPath,outputpath) = args

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


    val appNameCountRDD: RDD[(String, Iterable[String])] = logRDD.mapPartitions(itr => {
      val jedis = JedisPools.getJedis()
      // 遍历分区的所有数据，查询redis（把appname为空的数据进行转换），将结果放入到Listbuffer

      val listBuffer = new ListBuffer[((String, String))]
      itr.foreach(line => {
        val appname = line.getString(14)
        val appid = line.getString(13)
        val requestmode = line.getInt(8)
        val processnode = line.getInt(35)
        val iseffective = line.getInt(30)
        val isbilling = line.getInt(31)
        val isbid = line.getInt(39)
        val iswin = line.getInt(42)
        val adorderid = line.getInt(2)
        val winprice = line.getDouble(41)
        val adpayment = line.getDouble(75)

        var newAppname = ""
        if (appname.equals("未知")||appname.equals("其他")) {
          newAppname = jedis.get(appid)
          if (newAppname == null) {
            newAppname = appname
          }
        }else{
          newAppname = appname
        }
        val filterCondition = Constants.REQUESTMODE + "=" + requestmode + "|" + Constants.PROCESSNODE + "=" + processnode + "|" +
          Constants.ISEFFECTIVE + "=" + iseffective + "|" + Constants.ISBILLING + "=" + isbilling + "|" +
          Constants.ISBID + "=" + isbid + "|" + Constants.ISWIN + "=" + iswin + "|" + Constants.ADORDEERID + "=" + adorderid + "|" +
          Constants.WINPRICE + "=" + winprice + "|" + Constants.ADPAYMENT + "=" + adpayment

        listBuffer += ((newAppname, filterCondition))
      })
      listBuffer.iterator
    }).groupByKey

    val appnameAccumulatorRDD = FilterAndAccumulatorUtils.filterAndAccumulator(appNameCountRDD,logAccumulator)

    // 将累加后的字段拆分出来,并使用Row进行封装
   val appNameResultRDD: RDD[Row] = appnameAccumulatorRDD.map(tup => {
     val appname = tup._1

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
     Row(appname, original_request_count, effective_request_count, ad_request_count, join_biding_count, biding_win_count,
       biding_win_rate, show_count, click_count, click_rate, DSPwinprice, DSPadpayment)
   })

    val appNameDF = spark.createDataFrame(appNameResultRDD,SchemaUtils.appnameSchame)
    JDBC.createTable("appnameCountCore",appNameDF)







  }
}
