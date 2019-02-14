package accumulator

import constant.Constants
import org.apache.spark.util.AccumulatorV2
import util.StringUtils

/**
  * 自定义累加器，将要统计的变量使用累加器进行累加
  */
class LogAccumulator extends AccumulatorV2[String,String]{
  // 设置初始值
  var result =
    Constants.ORIGINAL_REQUEST_COUNT        +"=0|"+
    Constants.EFFECTIVE_REQUEST_COUNT       +"=0|"+
    Constants.AD_REQUEST_COUNT              +"=0|"+
    Constants.JOIN_BIDING_COUNT             +"=0|"+
    Constants.BIDING_WIN_COUNT              +"=0|"+
    Constants.SHOW_COUNT                    +"=0|"+
    Constants.CLICK_COUNT                   +"=0|"+
    Constants.DSPWINPRICE                   +"=0|"+
    Constants.DSPADPAYMENT                  +"=0"

  // 判断初始值是否为空
  override def isZero: Boolean = {
    true
  }

  // copy一个新的累加器
  override def copy(): AccumulatorV2[String, String] = {
    val myCopyAccumulotor = new LogAccumulator
    myCopyAccumulotor.result = this.result
    myCopyAccumulotor
  }

  // 重置一个累加器，将累加器中的数据初始化
  override def reset(): Unit = {
    result =
      Constants.ORIGINAL_REQUEST_COUNT        +"=0|"+
      Constants.EFFECTIVE_REQUEST_COUNT       +"=0|"+
      Constants.AD_REQUEST_COUNT              +"=0|"+
      Constants.JOIN_BIDING_COUNT             +"=0|"+
      Constants.BIDING_WIN_COUNT              +"=0|"+
      Constants.SHOW_COUNT                    +"=0|"+
      Constants.CLICK_COUNT                   +"=0|"+
      Constants.DSPWINPRICE                   +"=0|"+
      Constants.DSPADPAYMENT                  +"=0"
  }

  // 局部累加，给定具体的累加过程，属于每一个分区进行累加的方法
  override def add(v: String): Unit = {
    // v1是上一次聚合后的值
    val v1 = result
    // 这次传入的字段名称
    val v2 = v
    if (StringUtils.isNotEmpty(v1)&&StringUtils.isNotEmpty(v2)){
      var newResult = ""
      // 从v1中提取出v2对应的值并累加
      if(v2.startsWith("DSP")){
        val oldValue = StringUtils.getFieldFromConcatString(v1,"\\|",v2.split("_")(0))
        if (oldValue != null){
          val newValue = oldValue.toDouble + v2.split("_")(1).toDouble
          newResult = StringUtils.setFieldInConcatString(v1,"\\|",v2,String.valueOf(newValue))
        }
      }else{
        val oldValue = StringUtils.getFieldFromConcatString(v1,"\\|",v2)
        if (oldValue != null){
          val newValue = oldValue.toInt +1
          newResult = StringUtils.setFieldInConcatString(v1,"\\|",v2,String.valueOf(newValue))
        }
      }

      result = newResult
    }
  }

  // 全局累加
  override def merge(other: AccumulatorV2[String, String]): Unit = other match {
    case map: LogAccumulator =>{
      val fieldList = List(
        Constants.ORIGINAL_REQUEST_COUNT  ,
        Constants.EFFECTIVE_REQUEST_COUNT ,
        Constants.AD_REQUEST_COUNT        ,
        Constants.JOIN_BIDING_COUNT       ,
        Constants.BIDING_WIN_COUNT        ,
        Constants.SHOW_COUNT              ,
        Constants.CLICK_COUNT             ,
        Constants.DSPWINPRICE             ,
        Constants.DSPADPAYMENT
      )

      val res = other.value
      var value1 = 0
      var value2 = 0
      var aggrValue = ""
      for (elem <- fieldList) {
        if (elem.startsWith("DSP")){
          var value3 = 0.0
          var value4 = 0.0
          value3 = StringUtils.getFieldFromConcatString(res,"\\|",elem).toDouble
          value4 = StringUtils.getFieldFromConcatString(result,"\\|",elem).toDouble
          aggrValue = (value3+value4).toString
        }else{
          value1 = StringUtils.getFieldFromConcatString(res,"\\|",elem).toInt
          value2 = StringUtils.getFieldFromConcatString(result,"\\|",elem).toInt
          aggrValue = (value1+value2).toString
        }

        result = StringUtils.setFieldInConcatString(result,"\\|",elem,aggrValue)
      }
    }

    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: String = result
}
