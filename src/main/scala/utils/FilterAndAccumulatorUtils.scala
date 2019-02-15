package utils

import accumulator.LogAccumulator
import constant.Constants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import util.StringUtils

object FilterAndAccumulatorUtils {
  /**
    * 根据条件进行过滤，并实现累加
    * @param groupedRDD
    * @param logAccumulator
    */
  def filterAndAccumulator(groupedRDD: RDD[(String, Iterable[String])], logAccumulator: LogAccumulator): RDD[(String, String)] = {
    val accumulatorRDD: RDD[(String, String)] = groupedRDD.mapValues(itr => {
      logAccumulator.reset()
      val filterConditionlist = itr.iterator.toList
      for (filterCondition <- filterConditionlist) {

        // 使用自定义的工具类从拼接的字符串中取出相关的字段
        val requestmode = (StringUtils.getFieldFromConcatString(filterCondition, "\\|", Constants.REQUESTMODE)).toInt
        val processnode = (StringUtils.getFieldFromConcatString(filterCondition, "\\|", Constants.PROCESSNODE)).toInt
        val iseffective = (StringUtils.getFieldFromConcatString(filterCondition, "\\|", Constants.ISEFFECTIVE)).toInt
        val isbilling = (StringUtils.getFieldFromConcatString(filterCondition, "\\|", Constants.ISBILLING)).toInt
        val isbid = (StringUtils.getFieldFromConcatString(filterCondition, "\\|", Constants.ISBID)).toInt
        val iswin = (StringUtils.getFieldFromConcatString(filterCondition, "\\|", Constants.ISWIN)).toInt
        val adorderid = (StringUtils.getFieldFromConcatString(filterCondition, "\\|", Constants.ADORDEERID)).toInt
        val winprice = (StringUtils.getFieldFromConcatString(filterCondition, "\\|", Constants.WINPRICE)).toDouble
        val adpayment = (StringUtils.getFieldFromConcatString(filterCondition, "\\|", Constants.ADPAYMENT)).toDouble


        if ((requestmode == 1) && (processnode >= 1)) logAccumulator.add(Constants.ORIGINAL_REQUEST_COUNT)
        if ((requestmode == 1) && (processnode >= 2)) logAccumulator.add(Constants.EFFECTIVE_REQUEST_COUNT)
        if ((requestmode == 1) && (processnode == 3)) logAccumulator.add(Constants.AD_REQUEST_COUNT)
        if ((iseffective == 1) && (isbilling == 1) && (isbid == 1)) logAccumulator.add(Constants.JOIN_BIDING_COUNT)
        if ((iseffective == 1) && (isbilling == 1) && (iswin == 1) && (adorderid != 0)) logAccumulator.add(Constants.BIDING_WIN_COUNT)
        if ((requestmode == 2) && (iseffective == 1)) logAccumulator.add(Constants.SHOW_COUNT)
        if ((requestmode == 3) && (iseffective == 1)) logAccumulator.add(Constants.CLICK_COUNT)
        if ((iseffective == 1) && (isbilling == 1) && (iswin == 1)) logAccumulator.add(Constants.DSPWINPRICE + "_" + winprice)
        if ((iseffective == 1) && (isbilling == 1) && (iswin == 1)) logAccumulator.add(Constants.DSPADPAYMENT + "_" + adpayment)
      }
      logAccumulator.value
    })

    accumulatorRDD

  }

}
