package utils

/**
  * 将字符串转成Int或Double类型，如果出现异常返回零
  */
object NBF {
  def toInt(str:String):Int={
   try{
     str.toInt
   } catch {
     case _:Exception =>0
   }
  }
  def toDouble(str:String):Double={
    try{
      str.toDouble
    }catch {
      case _ :Exception => 0.0
    }
  }
}
