package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagCity extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    val provincename = row.getAs[String]("provincename")
    provincename match {
      case v  => list:+=("ZP"+v,1)
    }
    val cityname = row.getAs[String]("cityname")
    cityname match {
      case v  => list:+=("ZC"+v,1)
    }
    list
  }

}
