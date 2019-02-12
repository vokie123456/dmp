package etl

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import utils.{NBF, SchemaUtils}

/**
  * 处理数据，进行ETL
  */
object Bz2Parquet {
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

    // 开始读数据
    val lines = spark.sparkContext.textFile(inputPath)
    // 进行过滤,保证字段大于85，并且需要解析内部的,,,,,, 要进行特殊处理
    val rowRDD = lines
      .map(t=>t.split(",",t.length))  // 就可以将,,,,,,按,切分出来
      .filter(_.length>=85)
      .map(arr=>{
        Row(
          arr(0),
          NBF.toInt(arr(1)),
          NBF.toInt(arr(2)),
          NBF.toInt(arr(3)),
          NBF.toInt(arr(4)),
          arr(5),
          arr(6),
          NBF.toInt(arr(7)),
          NBF.toInt(arr(8)),
          NBF.toDouble(arr(9)),
          NBF.toDouble(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          NBF.toInt(arr(17)),
          arr(18),
          arr(19),
          NBF.toInt(arr(20)),
          NBF.toInt(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          NBF.toInt(arr(26)),
          arr(27),
          NBF.toInt(arr(28)),
          arr(29),
          NBF.toInt(arr(30)),
          NBF.toInt(arr(31)),
          NBF.toInt(arr(32)),
          arr(33),
          NBF.toInt(arr(34)),
          NBF.toInt(arr(35)),
          NBF.toInt(arr(36)),
          arr(37),
          NBF.toInt(arr(38)),
          NBF.toInt(arr(39)),
          NBF.toDouble(arr(40)),
          NBF.toDouble(arr(41)),
          NBF.toInt(arr(42)),
          arr(43),
          NBF.toDouble(arr(44)),
          NBF.toDouble(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          NBF.toInt(arr(57)),
          NBF.toDouble(arr(58)),
          NBF.toInt(arr(59)),
          NBF.toInt(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          NBF.toInt(arr(73)),
          NBF.toDouble(arr(74)),
          NBF.toDouble(arr(75)),
          NBF.toDouble(arr(76)),
          NBF.toDouble(arr(77)),
          NBF.toDouble(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          NBF.toInt(arr(84))
        )
      })

    val df = spark.createDataFrame(rowRDD,SchemaUtils.schema)

    // 存储为parquet  coalesce(1)重分区一定要慎用
    // 需求要求我们将清洗后的数据放到hdfs
    df.coalesce(1).write.parquet(outputPath)

    spark.stop()

  }
}
