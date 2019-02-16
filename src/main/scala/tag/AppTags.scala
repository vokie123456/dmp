package tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * 给app相关属性打上标签
  *
  */
object AppTags extends Tags {
  /**
    * 数据标签化接口
    *
    */
  override def makeTages(args: Any*): ListBuffer[(String, Int)] = {
    val list = new ListBuffer[(String,Int)]
    // 从传入的参数中获取数据
    val line = args(0).asInstanceOf[Row]
    val appdic = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    // 获取appname和appId
    val appname = line.getAs[String]("appname")
    val appid = line.getAs[String]("appid")
    if (StringUtils.isNotBlank(appname)){
      list +=(("APP"+appname,1))
    }else if (StringUtils.isNotBlank(appid)){
      list +=(("APP"+appdic.value.getOrElse(appid,appid),1))
    }
    list
  }
}
