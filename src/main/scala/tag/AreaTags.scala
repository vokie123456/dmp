package tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * 地域信息标签
  */
object AreaTags extends Tags {
  /**
    * 数据标签化接口
    *
    */
  override def makeTages(args: Any*): ListBuffer[(String, Int)] = {
    val line = args(0).asInstanceOf[Row]
    val list = new ListBuffer[(String,Int)]

    // 获取省份名称
    val provincename = line.getAs[String]("provincename")
    if (StringUtils.isNotBlank(provincename)){
      list +=((provincename,1))
    }

    val cityname = line.getAs[String]("cityname")
    if (StringUtils.isNotBlank(cityname)){
      list +=((cityname,1))
    }

    list
  }
}
