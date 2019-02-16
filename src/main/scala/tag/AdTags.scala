package tag

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer


object AdTags extends Tags {
  /**
    * 将广告相关属性打上标签
    *
    */
  override def makeTages(args: Any*): ListBuffer[(String, Int)] = {
    val line = args(0).asInstanceOf[Row]
    val list = new ListBuffer[(String,Int)]
    // 获取广告类型
    val adspacetype = line.getAs[Int]("adspacetype")
    adspacetype match {
      case v if v>9 => list += (("LC"+v,1))
      case v if v>0&&v<=9 => list += (("LC0"+v,1))
    }

    // 获取广告位类型名称
    val adspacetypename = line.getAs[String]("adspacetypename")
    if (StringUtils.isNotBlank(adspacetypename)){
      list += (("LN"+adspacetypename,1))
    }

    // 渠道标签
    val channel = line.getAs[Int]("adplatformproviderid")
    list+=(("CN"+channel,1))
    list

  }
}
