package graph

import com.typesafe.config.ConfigFactory
import constant.Constants
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import tag._
import utils.TagUtils

/**
  * 上下文标签，用来将所有标签进行汇总
  */
object TagsTodayFinal {
  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      println("目录不正确，退出程序")
      sys.exit()
    }

    // 接收参数
    val Array(inputPath, outputPath, dicPath, stopwords, day) = args

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val hbaseConf = HBaseConfiguration.create()
    val tablename = "dmp:fianltags"
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

    // 读取字典文件
    val dicMap = sc.textFile(dicPath).map(_.split("\t", -1)).filter(_.length >= 5)
      .map(arr => {
        (arr(4), arr(1))
      }).collect.toMap
    val dicMapBroadcast = sc.broadcast(dicMap)

    // 获取停用词库并广播
    val stopwordsDir = sc.textFile(stopwords).collect().toList
    val stopwordsBroadcast = sc.broadcast(stopwordsDir)

    // 读取数据
    val df = spark.read.parquet(inputPath)

    // 过滤需要的userId，因为userId很多，只需要过滤出userId不全为空的数据
    val result = df.filter(TagUtils.hasneedOneUserId)
      .rdd.map(row=>{
      // 处理上下文用户信息
      val userId = TagUtils.getRowAllUserId(row)
      (userId,row)
    })

    // 构建点的集合
    val vre = result.flatMap(tup=>{
      val line = tup._2

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
//      // 商圈标签
//      val buinessTagList = TagsBusiness.makeTages(line,jedis)
      val currentRowTag = (adTagList ++ appTagList ++ deviceTagList ++ keywordsTagList ++ areaTagList).toList
      val vd = tup._1.map((_,0)) ++ currentRowTag
      // 只有第一个人可以携带顶点VD，其他不需要
      // 如果同一行上有多个顶点VD，因为同一行数据都有一个用户
      // 将来肯定要聚合在一起的，这样就会造成重复的叠加了
      tup._1.map(uid=>{
        if (tup._1.head.equals(uid)){
          (uid.hashCode.toLong,vd)
        }else{
          (uid.hashCode.toLong,List.empty)
        }
      })
    })
      //    vre.take(20).foreach(println)
    // 构建边的集合
    val edges = result.flatMap(tup=>{
      tup._1.map(uid=>{
        Edge(tup._1.head.hashCode.toLong,uid.hashCode.toLong,0)
      })
    })

    //    edges.take(20).foreach(println)
    //图计算
    val graph = Graph(vre, edges)
    // 调用连通图算法，找到图中可以连通的分支
    // 并取出每个连通分支中最小的点的元组集合
    val cc = graph.connectedComponents().vertices
    //cc.take(20).foreach(println)
    // 认祖归宗
    val joined = cc.join(vre).map {
      case (uid, (commonId, tagsAndUserid)) => (commonId, tagsAndUserid)
    }
    val res = joined.reduceByKey {
      case (list1, list2) =>
        (list1 ++ list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }
//    res.take(20).foreach(println)

    //写入到HBASE中
    res.map {
      case (userid, userTags) => {
        val put = new Put(Bytes.toBytes(userid))
        val tags = userTags.map(t => t._1 + ":" + t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"),
          Bytes.toBytes(s"$day"), Bytes.toBytes(tags))
        (new ImmutableBytesWritable(), put)
      }
    }.saveAsHadoopDataset(jobconf)

    sc.stop()
  }

}
