package com.Rts

import com.utils.RptUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Equipment {
  def main(args: Array[String]): Unit = {

    // 判断路径是否正确
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 获取数据
    val df = sQLContext.read.parquet(inputPath)
    // 将数据进行处理，统计各个指标
    df.rdd.map(row=>{
      // 把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // key 值  是地域的省市
      var devicetype = row.getAs[Int]("devicetype")
      var devicename : String = null
        if (devicetype == 1){
          devicename = "手机"
        }else if (devicetype == 2){
          devicename  = "平板"
        }else {
          devicename = "其他"
        }
      val requestData = RptUtils.request(requestmode,processnode)
      val clickData = RptUtils.click(requestmode,iseffective)
      val AdData = RptUtils.Ad(iseffective,isbilling,isbid,iswin,adorderid,WinPrice,adpayment)
      val lists = requestData++clickData++AdData
      (devicename,lists)
    }).reduceByKey((x,y)=>{
      x.zip(y).map( t => t._1+t._2)
    }).saveAsTextFile(outputPath)



  }

}
