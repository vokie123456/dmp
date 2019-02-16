package app

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import tag.{AdTags, AppTags, DeviceTags, KeyWordsTags}

import scala.collection.mutable.ListBuffer

object DataTag {

  def main(args: Array[String]): Unit = {

    // 模拟企业级编程 首先判断目录是否为空
    if(args.length!=4){
      println("目录不正确，退出程序")
      sys.exit()
    }

    // 创建一个集合存储输入输出目录
    val Array(inputPath,appdisctPath,stopwordsPath,outPath) = args

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
    val logRDD = df.rdd.cache()

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
    tagRDD.saveAsTextFile(outPath)
  }


  /**
    * 给数据打上标签
    *
    * @param logRDD
    * @return
    */
  def getDataTag(logRDD:RDD[Row],dicMapBroadcast: Broadcast[Map[String, String]],stopwordsBroadcast: Broadcast[List[String]]) = {
    val tagResultRDD: RDD[String] = logRDD.map(line => {
      val userid = line.getAs[String]("userid")
      val tagList = new ListBuffer[((String, Int))]
      val adTagList = AdTags.makeTages(line)
      val appTagList = AppTags.makeTages(line, dicMapBroadcast)
      val deviceTagList = DeviceTags.makeTages(line)
      val keywordsTagList = KeyWordsTags.makeTages(line, stopwordsBroadcast)

      tagList ++= (adTagList ++ appTagList ++ deviceTagList ++ keywordsTagList)

      (userid, tagList)

    }).reduceByKey(reducefunc)
      .map(tup => {
        val resultBuffer = new StringBuffer
        val userId = tup._1
        resultBuffer.append(userId + " ")
        val taglist = tup._2
        for (tagTup <- taglist) {
          resultBuffer.append(tagTup._1 + " " + tagTup._2 + " ")
        }
        resultBuffer.toString
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