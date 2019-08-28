package com.Exam

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SparkSession

object ExamMain {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    if (args.length != 1){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val  Array(inputPath) = args
    // 读取数据
    val ds = spark.read.textFile(inputPath).collect()
    ds.map(x=> {
      val jsonparse = JSON.parseObject(x)
      val status = jsonparse.getIntValue("status")
      if(status==0) return ""
      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""
      val poisJson = regeocodeJson.getJSONArray("pois")
      if (poisJson == null || poisJson.isEmpty) return null
      var list = List[(String,Int)]()
      for(item <- poisJson.toArray){
        if (item.isInstanceOf[JSONObject]){
          val json = item.asInstanceOf[JSONObject]
            list:+=(json.getString("businessarea"),1)
        }
      }
      println(list.groupBy(_._1).mapValues(_.size).toBuffer)
    })
  }
  }

