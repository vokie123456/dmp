package tag
import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * 将数据中获取的经纬度信息转化为商圈信息
  */
object TagsBusiness extends Tags {
  /**
    * 数据标签化接口
    *
    */
  override def makeTages(args: Any*): ListBuffer[(String, Int)] = {
    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]
    val list = new ListBuffer[(String,Int)]
    if (row.getAs[String]("long").toDouble >= 73.66 && row.getAs[String]("long").toDouble <= 135.05 &&
      row.getAs[String]("lat").toDouble >= 3.86 && row.getAs[String]("lat").toDouble <= 53.55) {
      val lat = row.getAs[String]("lat")  // 获取纬度
      val long = row.getAs[String]("long")// 获取经度
      val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble, long.toDouble, 8) // 通过geohash将数据中的经纬度转化为8位的地理位置字符串
      val business = jedis.get(geoHash)
      business.split(";").foreach(t=>list += ((t,1)))
    }
    list

  }
}
