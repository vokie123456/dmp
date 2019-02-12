package beans

import utils.NBF

/**
  * 定义了日志的所有属性，包括所有字段的意思和数据类型
  */
class Log (
            val sessionid: String,
            val advertisersid: Int,
            val adorderid: Int,
            val adcreativeid: Int,
            val adplatformproviderid: Int,
            val sdkversion: String,
            val adplatformkey: String,
            val putinmodeltype: Int,
            val requestmode: Int,
            val adprice: Double,
            val adppprice: Double,
            val requestdate: String,
            val ip: String,
            val appid: String,
            val appname: String,
            val uuid: String,
            val device: String,
            val client: Int,
            val osversion: String,
            val density: String,
            val pw: Int,
            val ph: Int,
            val _long: String,
            val lat: String,
            val provincename: String,
            val cityname: String,
            val ispid: Int,
            val ispname: String,
            val networkmannerid: Int,
            val networkmannername: String,
            val iseffective: Int,
            val isbilling: Int,
            val adspacetype: Int,
            val adspacetypename: String,
            val devicetype: Int,
            val processnode: Int,
            val apptype: Int,
            val district: String,
            val paymode: Int,
            val isbid: Int,
            val bidprice: Double,
            val winprice: Double,
            val iswin: Int,
            val cur: String,
            val rate: Double,
            val cnywinprice: Double,
            val imei: String,
            val mac: String,
            val idfa: String,
            val openudid: String,
            val androidid: String,
            val rtbprovince: String,
            val rtbcity: String,
            val rtbdistrict: String,
            val rtbstreet: String,
            val storeurl: String,
            val realip: String,
            val isqualityapp: Int,
            val bidfloor: Double,
            val aw: Int,
            val ah: Int,
            val imeimd5: String,
            val macmd5: String,
            val idfamd5: String,
            val openudidmd5: String,
            val androididmd5: String,
            val imeisha1: String,
            val macsha1: String,
            val idfasha1: String,
            val openudidsha1: String,
            val androididsha1: String,
            val uuidunknow: String,
            val userid: String,
            val iptype: Int,
            val initbidprice: Double,
            val adpayment: Double,
            val agentrate: Double,
            val lomarkrate: Double,
            val adxrate: Double,
            val title: String,
            val keywords: String,
            val tagid: String,
            val callbackdate: String,
            val channelid: String,
            val mediatype: Int
          ) extends Product with Serializable {
  // 角标和成员属性的映射关系
  override def productElement(n: Int): Any = n match {
    case 0	=> sessionid
    case 1	=> advertisersid
    case 2	=> adorderid
    case 3	=> adcreativeid
    case 4	=> adplatformproviderid
    case 5	=> sdkversion
    case 6	=> adplatformkey
    case 7	=> putinmodeltype
    case 8	=> requestmode
    case 9	=> adprice
    case 10	=> adppprice
    case 11	=> requestdate
    case 12	=> ip
    case 13	=> appid
    case 14	=> appname
    case 15	=> uuid
    case 16	=> device
    case 17	=> client
    case 18	=> osversion
    case 19	=> density
    case 20	=> pw
    case 21	=> ph
    case 22	=> _long
    case 23	=> lat
    case 24	=> provincename
    case 25	=> cityname
    case 26	=> ispid
    case 27	=> ispname
    case 28	=> networkmannerid
    case 29	=> networkmannername
    case 30	=> iseffective
    case 31	=> isbilling
    case 32	=> adspacetype
    case 33	=> adspacetypename
    case 34	=> devicetype
    case 35	=> processnode
    case 36	=> apptype
    case 37	=> district
    case 38	=> paymode
    case 39	=> isbid
    case 40	=> bidprice
    case 41	=> winprice
    case 42	=> iswin
    case 43	=> cur
    case 44	=> rate
    case 45	=> cnywinprice
    case 46	=> imei
    case 47	=> mac
    case 48	=> idfa
    case 49	=> openudid
    case 50	=> androidid
    case 51	=> rtbprovince
    case 52	=> rtbcity
    case 53	=> rtbdistrict
    case 54	=> rtbstreet
    case 55	=> storeurl
    case 56	=> realip
    case 57	=> isqualityapp
    case 58	=> bidfloor
    case 59	=> aw
    case 60	=> ah
    case 61	=> imeimd5
    case 62	=> macmd5
    case 63	=> idfamd5
    case 64	=> openudidmd5
    case 65	=> androididmd5
    case 66	=> imeisha1
    case 67	=> macsha1
    case 68	=> idfasha1
    case 69	=> openudidsha1
    case 70	=> androididsha1
    case 71	=> uuidunknow
    case 72	=> userid
    case 73	=> iptype
    case 74	=> initbidprice
    case 75	=> adpayment
    case 76	=> agentrate
    case 77	=> lomarkrate
    case 78	=> adxrate
    case 79	=> title
    case 80	=> keywords
    case 81	=> tagid
    case 82	=> callbackdate
    case 83	=> channelid
    case 84	=> mediatype
  }

  // 对象一共有都少个成员
  override def productArity: Int = 85

  // 比较两个对象是否是同一个对象
  override def canEqual(that: Any): Boolean = this.isInstanceOf[Log]
}

object Log{
  def apply(arr: Array[String]): Log = new Log(
    arr(0),
    NBF.toInt(arr(1)),
    NBF.toInt(arr(2)),
    NBF.toInt(arr(3)),
    NBF.toInt(arr(4)),
    arr(5),
    arr(6),
    NBF.toInt(arr(7)),
    NBF.toInt(arr(8)),
    NBF.toDouble(arr(9)),
    NBF.toDouble(arr(10)),
    arr(11),
    arr(12),
    arr(13),
    arr(14),
    arr(15),
    arr(16),
    NBF.toInt(arr(17)),
    arr(18),
    arr(19),
    NBF.toInt(arr(20)),
    NBF.toInt(arr(21)),
    arr(22),
    arr(23),
    arr(24),
    arr(25),
    NBF.toInt(arr(26)),
    arr(27),
    NBF.toInt(arr(28)),
    arr(29),
    NBF.toInt(arr(30)),
    NBF.toInt(arr(31)),
    NBF.toInt(arr(32)),
    arr(33),
    NBF.toInt(arr(34)),
    NBF.toInt(arr(35)),
    NBF.toInt(arr(36)),
    arr(37),
    NBF.toInt(arr(38)),
    NBF.toInt(arr(39)),
    NBF.toDouble(arr(40)),
    NBF.toDouble(arr(41)),
    NBF.toInt(arr(42)),
    arr(43),
    NBF.toDouble(arr(44)),
    NBF.toDouble(arr(45)),
    arr(46),
    arr(47),
    arr(48),
    arr(49),
    arr(50),
    arr(51),
    arr(52),
    arr(53),
    arr(54),
    arr(55),
    arr(56),
    NBF.toInt(arr(57)),
    NBF.toDouble(arr(58)),
    NBF.toInt(arr(59)),
    NBF.toInt(arr(60)),
    arr(61),
    arr(62),
    arr(63),
    arr(64),
    arr(65),
    arr(66),
    arr(67),
    arr(68),
    arr(69),
    arr(70),
    arr(71),
    arr(72),
    NBF.toInt(arr(73)),
    NBF.toDouble(arr(74)),
    NBF.toDouble(arr(75)),
    NBF.toDouble(arr(76)),
    NBF.toDouble(arr(77)),
    NBF.toDouble(arr(78)),
    arr(79),
    arr(80),
    arr(81),
    arr(82),
    arr(83),
    NBF.toInt(arr(84))
  )
}
