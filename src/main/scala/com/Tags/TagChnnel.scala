package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagChnnel extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val chnnel = row.getAs[Int]("adplatformproviderid")
    chnnel match {
      case a =>list:+=("CN"+a,1)
    }
    list
  }
}
