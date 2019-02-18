package utils

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 用来获取userid的工具类
  */
object TagUtils {
  // 处理不同的用户信息
  def getAnyOneUserId(row:Row)={
    row match {
      case v if StringUtils.isNotBlank(v.getAs[String]("imei")) =>
        "IM" + v.getAs[String]("imei")
      case v if StringUtils.isNotBlank(v.getAs[String]("mac")) =>
        "MC" + v.getAs[String]("mac")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfa")) =>
        "ID" + v.getAs[String]("idfa")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudid")) =>
        "OD" + v.getAs[String]("openudid")
      case v if StringUtils.isNotBlank(v.getAs[String]("androidid")) =>
        "AOD" + v.getAs[String]("androidid")
      case v if StringUtils.isNotBlank(v.getAs[String]("imeimd5")) =>
        "MD5IM" + v.getAs[String]("imeimd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("macmd5")) =>
        "MD5MC" + v.getAs[String]("macmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidmd5")) =>
        "MD5OD" + v.getAs[String]("openudidmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididmd5")) =>
        "MD5AOD" + v.getAs[String]("androididmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfamd5")) =>
        "MD5ID" + v.getAs[String]("idfamd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("imeisha1")) =>
        "SHIM" + v.getAs[String]("imeisha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("macsha1")) =>
        "SHMC" + v.getAs[String]("macsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfasha1")) =>
        "SHID" + v.getAs[String]("idfasha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidsha1")) =>
        "SHOD" + v.getAs[String]("openudidsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididsha1")) =>
        "SHAOD" + v.getAs[String]("androididsha1")
    }
  }

  val hasneedOneUserId =
    "imei!='' " +
    "or mac!='' " +
    "or idfa!='' " +
    "or openudid!='' " +
    "or androidid!='' " +
    "or imeimd5!='' " +
    "or macmd5!= '' " +
    "or openudidmd5!='' " +
    "or idfamd5!='' " +
    "or androididmd5!='' " +
    "or imeisha1!='' " +
    "or macsha1!='' " +
    "or idfasha1!='' " +
    "or openudidsha1!='' " +
    "or androididsha1!=''"

  val hasneedOneUserId1 =
    "cast(long as double)>=73 and cast(long as double)<=136 " +
      "and cast(lat as double)>=3 and cast(lat as double)<=54 " +
      "and (imei!='' " +
      "or mac!='' " +
      "or idfa!='' " +
      "or openudid!='' " +
      "or androidid!='' " +
      "or imeimd5!='' " +
      "or macmd5!='' " +
      "or idfamd5!='' " +
      "or openudidmd5!='' " +
      "or androididmd5!='' " +
      "or imeisha1!='' " +
      "or macsha1!='' " +
      "or idfasha1!='' " +
      "or openudidsha1!='' " +
      "or androididsha1!='')"

  // 处理数据中的所有的用户信息
  def getRowAllUserId(row: Row): List[String] = {
    var list = List[String]()
    if (StringUtils.isNotBlank(row.getAs[String]("imei")))
      list :+= "IM:" + row.getAs[String]("imei")
    if (StringUtils.isNotBlank(row.getAs[String]("mac")))
      list :+= "MC:" + row.getAs[String]("mac")
    if (StringUtils.isNotBlank(row.getAs[String]("idfa")))
      list :+= "ID:" + row.getAs[String]("idfa")
    if (StringUtils.isNotBlank(row.getAs[String]("openudid")))
      list :+= "OD:" + row.getAs[String]("openudid")
    if (StringUtils.isNotBlank(row.getAs[String]("androidid")))
      list :+= "AOD:" + row.getAs[String]("androidid")
    if (StringUtils.isNotBlank(row.getAs[String]("imeimd5")))
      list :+= "IMM:" + row.getAs[String]("imeimd5")
    if (StringUtils.isNotBlank(row.getAs[String]("macmd5")))
      list :+= "MCM:" + row.getAs[String]("macmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("idfamd5")))
      list :+= "IDM:" + row.getAs[String]("idfamd5")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidmd5")))
      list :+= "ODM:" + row.getAs[String]("openudidmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("androididmd5")))
      list :+= "AODM:" + row.getAs[String]("androididmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("imeisha1")))
      list :+= "IMS:" + row.getAs[String]("imeisha1")
    if (StringUtils.isNotBlank(row.getAs[String]("macsha1")))
      list :+= "MCS:" + row.getAs[String]("macsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("idfasha1")))
      list :+= "IDS:" + row.getAs[String]("idfasha1")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidsha1")))
      list :+= "ODS:" + row.getAs[String]("openudidsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("androididsha1")))
      list :+= "AODS:" + row.getAs[String]("androididsha1")

    list
  }
}
