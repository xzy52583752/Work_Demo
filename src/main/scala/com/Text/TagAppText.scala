package com.Text

import com.utils.{JedisConnectionPool, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagAppText extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    // 处理参数类型
    val row = args(0).asInstanceOf[Row]

    val jedis =JedisConnectionPool.getConnection()
    // 获取APPid和APPname
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    // 空值判断
    if(StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else if(StringUtils.isNoneBlank(appid)){
      list:+=("APP"+jedis.get(appid),1)
    }
    jedis.close()
    list
  }
}
