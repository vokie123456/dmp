package tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * 给设备相关的属性打上标签
  */
object DeviceTags extends Tags {
  /**
    * 数据标签化接口
    *
    */
  override def makeTages(args: Any*): ListBuffer[(String, Int)] = {
    val line = args(0).asInstanceOf[Row]
    val list = new ListBuffer[(String,Int)]

    // 获取操作系统
    val os = line.getAs[Int]("client")
    if (os != null){
      os match {
        case 1 => list += (("D0001000"+os,1)) // Android
        case 2 => list += (("D0001000"+os,1)) // IOS
        case 3 => list += (("D0001000"+os,1)) // WP
        case _ => list += (("D00010004",1))   // 其他
      }
    }

    // 获取联网方式
    val networkmannername = line.getAs[String]("networkmannername")

    networkmannername match {
        case "WIFI" => list += (("D00020001",1))
        case "4G" => list += (("D00020002",1))
        case "3G" => list += (("D00020003",1))
        case "2G" => list += (("D00020004",1))
        case _ => list += (("D00020005",1))
      }



    // 获取运营商
    val ispname = line.getAs[String]("ispname")
    ispname match{
      case "移动" => list += (("D00030001",1))
      case "联通" => list += (("D00030002",1))
      case "电信" => list += (("D00030003",1))
      case _ => list += (("D00030004",1))


    }
    list
  }
}
