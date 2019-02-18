package tools
import java.io.UnsupportedEncodingException
import java.net.URLEncoder
import java.security.NoSuchAlgorithmException
import java.util

import com.google.gson.{JsonObject, JsonParser}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.lang3.StringUtils


/**
  * 请求百度LBS，解析经纬度对应的商圈信息
  */
object BaiduLBSHandler {
  /**
    * 对外提供的解析经纬度对应的商圈信息
    * @param lng 经度
    * @param lat 纬度
    */
  def parseBusinessTagBy(lng: String, lat: String) = {
    // 用于存储商圈信息
    var business: String = ""
    val requestParams = requetParams(lng, lat)
    val requestURL = "http://api.map.baidu.com/geocoder/v2/?" + requestParams
    // 使用HttpClient 模拟浏览器发送请求
    val httpClient = new HttpClient()
    val getMethod = new GetMethod(requestURL)
    val statusCode = httpClient.executeMethod(getMethod)
    if (statusCode == 200) { // HTTP.OK
      val response = getMethod.getResponseBodyAsString

      // 判断是否是合法的json字符换
      var str = response.replaceAll("renderReverse&&renderReverse\\(", "")
      if (!response.startsWith("{")) {
        str = str.substring(0, str.length - 1)
      }

      // 解析这个json字符串，取出business节点数据
      val returnData = new JsonParser().parse(str).getAsJsonObject

      // 服务器返回来的json数据，status表示服务器是否正常(0)处理了我的请求
      val status = returnData.get("status").getAsInt
      if (status == 0) {
        val resultObject = returnData.getAsJsonObject("result")
        business = resultObject.get("business").getAsString.replaceAll(",", ";")

        // 判断business是否为空，如果为空，接着解析改坐标点附近的标签信息pois
        if (StringUtils.isEmpty(business)) {
          val pois = resultObject.getAsJsonArray("pois")
          var tagSet = Set[String]()
          for (i <- 0 until pois.size()) {
            val elemObject: JsonObject = pois.get(i).getAsJsonObject
            val tag = elemObject.get("tag").getAsString
            if (StringUtils.isNotEmpty(tag)) tagSet += tag
          }
          business = tagSet.mkString(";")
        }
      }
    }
    business
  }

  private def requetParams(lng: String, lat: String) = {

    // 应用AK：ceZoLPqAOwDPyMcBi0PpSIe6W3GznF2x
    // SK：pW5fwnxB2M1EOaamuC2DiE37iiwgobRO
    val list = "rkWtTiHEOmn6PbY02nxHFMv4U4svsTxu,1m2XKkskSNhNznpzXyBbCzo9hYGGVV3j"

    val Array(ak, sk) = list.split(",")

    // 计算sn跟参数对出现顺序有关，get请求请使用LinkedHashMap保存<key,value>，
    // 该方法根据key的插入顺序排序；post请使用TreeMap保存<key,value>，
    // 该方法会自动将key按照字母a-z顺序排序。所以get请求可自定义参数顺序（sn参数必须在最后）发送请求，
    // 但是post请求必须按照字母a-z顺序填充body（sn参数必须在最后）。
    // 以get请求为例：http://api.map.baidu.com/geocoder/v2/?address=百度大厦&output=json&ak=yourak，
    // paramsMap中先放入address，再放output，然后放ak，放入顺序必须跟get请求中对应参数的出现顺序保持一致。
    val paramsMap = new util.LinkedHashMap[String, String]();
    paramsMap.put("callback", "renderReverse")
    // paramsMap.put("location", "39.343424,116.452987")
    paramsMap.put("location", lat.concat(",").concat(lng))
    paramsMap.put("output", "json")
    paramsMap.put("pois", "1")
    paramsMap.put("ak", ak)

    // 请求的参数
    val paramsStr = toQueryString(paramsMap)

    // 生成SN
    val wholeStr = new String("/geocoder/v2/?" + paramsStr + sk)
    val tempStr = URLEncoder.encode(wholeStr, "UTF-8")
    val sn = MD5(tempStr)

    paramsStr + "&sn=" + sn
  }

  // 对Map内所有value作utf8编码，拼接返回结果
  @throws[UnsupportedEncodingException]
  private def toQueryString(data: util.LinkedHashMap[String, String]): String = {
    val queryString = new StringBuffer
    import scala.collection.JavaConversions._
    for (pair <- data.entrySet) {
      queryString.append(pair.getKey + "=")
      queryString.append(URLEncoder.encode(pair.getValue.asInstanceOf[String], "UTF-8") + "&")
    }
    if (queryString.length > 0) queryString.deleteCharAt(queryString.length - 1)
    queryString.toString
  }

  // 来自stackoverflow的MD5计算方法，调用了MessageDigest库函数，并把byte数组结果转换成16进制
  private def MD5(md5: String): String = {
    try {
      val md = java.security.MessageDigest.getInstance("MD5")
      val array = md.digest(md5.getBytes)
      val sb = new StringBuffer
      var i = 0
      while ( {
        i < array.length
      }) {
        sb.append(Integer.toHexString((array(i) & 0xFF) | 0x100).substring(1, 3))

        {
          i += 1
          i
        }
      }
      return sb.toString
    } catch {
      case e: NoSuchAlgorithmException =>

    }
    null
  }


}
