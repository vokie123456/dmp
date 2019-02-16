package tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * 关键字标签
  */
object KeyWordsTags extends Tags {
  /**
    * 数据标签化接口
    *
    */
  override def makeTages(args: Any*): ListBuffer[(String, Int)] = {
    val list = new ListBuffer[(String,Int)]
    // 从传入的参数中获取数据
    val line = args(0).asInstanceOf[Row]
    val stopwords = args(1).asInstanceOf[Broadcast[List[String]]]
    // 获取数据中的关键字
    val keywords = line.getAs[String]("keywords")
    if (StringUtils.isNotBlank(keywords)){
      if (!keywords.contains("|")){
        if (keywords.length>=3&&keywords.length<8&& (!stopwords.value.contains(keywords)))
          list +=(("K"+keywords,1))
      }else{
        val fields = keywords.split("\\|",-1)
        for (field <- fields){
          if (field.length>=3&&field.length<8&& (!stopwords.value.contains(keywords)))
            list +=(("K"+field,1))
        }

      }
    }

    list

  }
}
