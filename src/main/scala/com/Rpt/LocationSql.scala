package com.Rpt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

object LocationSql {
  def main(args: Array[String]): Unit = {
    // 判断路径是否正确
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath) = args
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = spark.read.parquet(inputPath)
    sc.createOrReplaceTempView("temp")
    val s1= spark.sql("select " +
      "provincename , " +
      "cityname , " +
      "sum ( case when ((requestmode = 1) and (processnode >= 1)) then 1 else 0 end) as `原始请求数` , " +
      "sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as `有效请求数` , " +
      "sum(case when requestmode = 1 and processnode >= 3 then 1 else 0 end) as `广告请求数` , " +
      "sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) as `参与竞价数` , " +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) as `竞价成功数` , " +
      "sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as `展示数` , " +
      "sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) as `点击数` , " +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winPrice/1000 else 0 end)as `DSP广告消费` , " +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) as `DSP广告成本`  " +
      "from temp  " +
      "group by provincename,cityname")
//    s1.coalesce(1).write.json(outputPath)
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    s1.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),"territory",prop)
    spark.stop()
  }
}
