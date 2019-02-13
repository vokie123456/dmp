package utils

import java.util.Properties

import org.apache.spark.sql.DataFrame

object JDBC {
  def createTable(tablename:String,df:DataFrame)={
    // 创建properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456abcd")
    val url = "jdbc:mysql://192.168.58.145:3306/dmp?useUnicode=true&characterEncoding=utf8"
    df.write.jdbc(url,tablename,prop)
  }
}
