package com.Tags

import com.utils.TagUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TagsContext {
  def main(args: Array[String]): Unit = {
    if (args.length !=4){
      println("目录不匹配，退出程序")
      sys.exit()
    }

    val Array(inputPath,outputPath,dirPath,stopPath) = args


    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sQLContext = new SQLContext(sc)
    // 读取数据
    val df = sQLContext.read.parquet(inputPath)

    val map = sc.textFile(dirPath).map(_.split("\t")).filter(_.length>=5).map(x=>(x(4),x(1))).collectAsMap()
    val boardcast = sc.broadcast(map)

    val stop = sc.textFile(stopPath).map((_,0)).collectAsMap()
    val stoppath = sc.broadcast(stop)

    // 过滤符合Id的数据
    df.filter(TagUtils.OneUserId)
      // 接下来所有的标签都在内部实现
      .rdd.map(row=>{
      // 取出用户Id
      val userId = TagUtils.getOneUserId(row)
      // 接下来通过row数据 打上 所有标签（按照需求）
      val adList = TagsAd.makeTags(row)
      val chnnelList = TagChnnel.makeTags(row)
      val emlist = TagEm.makeTags(row)
      val applist = TagApp.makeTags(row,boardcast)
      val keyList = TagKeyWord.makeTags(row,stoppath)
      val cityList = TagCity.makeTags(row)
//
      (adList++chnnelList++applist++emlist++keyList++cityList)
//      (userId++adList++chnnelList++emlist)
    }).saveAsTextFile(outputPath)



  }

}
