package tag

import scala.collection.mutable.ListBuffer

trait Tags {
  /**
    * 数据标签化接口
    *
    */
  def makeTages(args:Any*):ListBuffer[(String,Int)]

}
