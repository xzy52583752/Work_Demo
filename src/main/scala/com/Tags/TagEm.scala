package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagEm extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val client = row.getAs[Int]("client")
    if (client==1) {list:+=("Android",1)}
    else if (client==2) { list:+=("ios",2)}
    else{list:+=("其他",3)}
    val networkmannerid = row.getAs[Int]("networkmannerid")
    val networkmannername = row.getAs[String]("networkmannername")
     if (networkmannerid==1) { list:+=("2G",1)}
        else if (networkmannerid==2) { list:+=("3G",2)}
        else if (networkmannerid==3){list:+=("Wifi",4)}
        else if (networkmannerid==4) { list:+=("4G",3)}
    val ispname = row.getAs[String]("ispname")
    val ispid = row.getAs[Int]("ispid")
    if (ispid==1) { list:+=("移动", 1)}
    else if (ispid==2) {list:+=("联通", 2)}
    else if (ispid==3) {list:+=("电信", 3)}
    else {list:+=("其他", 4)}

    list
  }
}
