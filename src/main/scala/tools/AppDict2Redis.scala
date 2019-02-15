package tools


import org.apache.spark.{SparkConf, SparkContext}
import utils.JedisPools

/**
  * 将字典库存入到redis中
  */
object AppDict2Redis {

    def main(args: Array[String]): Unit = {

        // 0 校验参数个数
        if (args.length != 1) {
            println(
                """
                  |cn.dmp.tools.AppDict2Redis
                  |参数：
                  | appdictInputPath
                """.stripMargin)
            sys.exit()
        }

        // 1 接受程序参数
        val Array(appdictInputPath) = args

        // 2 创建sparkconf->sparkContext
        val sparkConf = new SparkConf()
        sparkConf.setAppName(s"${this.getClass.getSimpleName}")
        sparkConf.setMaster("local[*]")
        // RDD 序列化到磁盘 worker与worker之间的数据传输
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(sparkConf)

        sc.textFile(appdictInputPath)
          .map(t=>t.split("\t",t.length))  // 就可以将,,,,,,按,切分出来
          .filter(arr=>{
            arr.length >= 6 && !arr(4).equals("")
          }).map(arr=>{
            (arr(4),arr(1))
          }).foreachPartition(itr => {

            val jedis = JedisPools.getJedis()

            itr.foreach(t => {
                jedis.set(t._1, t._2)
            })

            jedis.close()
        })


        sc.stop()
    }

}
