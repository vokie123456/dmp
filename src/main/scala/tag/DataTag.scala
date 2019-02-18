package tag

import constant.Constants
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import utils.{JedisPools, TagUtils}

import scala.collection.mutable.ListBuffer

/**
  * 将数据打标签，并对标签进行整合
  */
object DataTag {

  def main(args: Array[String]): Unit = {

    // 模拟企业级编程 首先判断目录是否为空
    if(args.length!=5){
      println("目录不正确，退出程序")
      sys.exit()
    }

    // 创建一个集合存储输入输出目录
    val Array(inputPath,appdisctPath,stopwordsPath,outPath,day) = args

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val df = spark.read.parquet(inputPath)

    // 过滤需要的userid，因为userid很多，只需要过滤出userId不全为空的数据
    val filteredDF = df.filter(TagUtils.hasneedOneUserId)
    val logRDD = filteredDF.rdd.cache()

    // 读取字典文件
    val dicMap = sc.textFile(appdisctPath)
      .map(_.split("\t",-1))
      .filter(_.length>=6)
      .map(arr=>{
        // com.123.cn   爱奇艺
        (arr(4),arr(1))
      }).collect().toMap
    val dicMapBroadcast: Broadcast[Map[String, String]] = sc.broadcast(dicMap)

    // 读取停止词汇文件
    val stopwordslist = sc.textFile(stopwordsPath)
      .collect().toList
    val stopwordsBroadcast: Broadcast[List[String]] = sc.broadcast(stopwordslist)


    // 给数据打标签
    val tagRDD =  getDataTag(logRDD,dicMapBroadcast,stopwordsBroadcast)

    spark2HBase(tagRDD,day)
//    tagRDD.saveAsTextFile(outPath)


    spark.stop()
  }

  /**
    * 将结果标签数据写入到hbase中
    * @param tagRDD
    * @return
    */
  def spark2HBase(tagRDD:RDD[(String,String)],day:String) = {
    val hbaseConf = HBaseConfiguration.create()
    val tablename = "dmp:tags"
    hbaseConf.set(Constants.zkQuorm, Constants.zkQuormlist)
    hbaseConf.set(Constants.zkQuormProt,Constants.clientPort)
    hbaseConf.set("hbase.table.sanity.checks","false")

    val jobconf = new JobConf(hbaseConf)
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    // 创建一个列簇
    val columnDescriptor = new HColumnDescriptor("tags")
    // 如果表不存在则创建表
    val admin = new HBaseAdmin(hbaseConf)
    if (!admin.isTableAvailable(tablename)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
      tableDesc.addFamily(columnDescriptor)
      admin.createTable(tableDesc)
    }



    tagRDD.map(tup=>{
      val put = new Put(Bytes.toBytes(tup._1))
      put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(s"$day"), Bytes.toBytes(tup._2))

      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobconf)



  }


  /**
    * 给数据打上标签
    *
    * @param logRDD
    * @return
    */
  def getDataTag(logRDD:RDD[Row],dicMapBroadcast: Broadcast[Map[String, String]],stopwordsBroadcast: Broadcast[List[String]]) = {
    val tagResultRDD = logRDD.mapPartitions(itr=>{
      val jedis = JedisPools.getJedis()
      itr.map(
        line => {
          // 处理一下userId，只拿到第一个不为空的userId作为这条数据的用户标识（userId）
          val userId = TagUtils.getAnyOneUserId(line)
          val tagList = new ListBuffer[((String, Int))]
          // 根据每一条数据  打上对应的标签信息（7种标签）
          // 开始打标签
          // 广告标签和渠道
          val adTagList = AdTags.makeTages(line)
          // APP标签
          val appTagList = AppTags.makeTages(line, dicMapBroadcast)
          // 设备
          val deviceTagList = DeviceTags.makeTages(line)
          // 关键字
          val keywordsTagList = KeyWordsTags.makeTages(line, stopwordsBroadcast)
          // 地域标签
          val areaTagList = AreaTags.makeTages(line)
          // 商圈标签
          val buinessTagList = TagsBusiness.makeTages(line,jedis)
          println(buinessTagList)


          tagList ++= (adTagList ++ appTagList ++ deviceTagList ++ keywordsTagList ++ areaTagList++buinessTagList)

          (userId, tagList)

        }
      )
    }).reduceByKey(reducefunc)
      .map(tup => {
        val resultBuffer = new StringBuffer
        val userId = tup._1+" "
        resultBuffer.append(userId)
        val taglist = tup._2
        for (tagTup <- taglist) {
          resultBuffer.append(tagTup._1 + ":" + tagTup._2 + " ")
        }
        (userId,resultBuffer.toString)
//        resultBuffer.toString
      })
    tagResultRDD
  }

  /**
    * 用来进行聚合，将用户的所有相关的数据进行聚合
    * @return
    */

  def reducefunc(list1:ListBuffer[(String,Int)],list2:ListBuffer[(String,Int)])= {
    val taglist = new ListBuffer[(String,Int)]
    var flag = true
    for (list1elem<- list1) {
      import scala.util.control.Breaks._
      breakable{
        for (list2elem <- list2) {
        // 如果两个list中的元组的key相同则进行相加
          if (list2elem._1.equals(list1elem._1)){
            taglist += ((list1elem._1,list1elem._2+list2elem._2))
            flag = false

            break
          }
        }
      }
      // 代码运行到这里说明list1中有的元素list2中没有
      if (flag){
        taglist+=list1elem
        flag = true
      }
    }

    val tagkeyList = new ListBuffer[String]
    for (tagelem <- taglist) {
      tagkeyList += tagelem._1
    }
    for (elem <- list2) {
      if (!tagkeyList.contains(elem._1)){
        taglist += elem
      }
    }

    taglist
  }



}
