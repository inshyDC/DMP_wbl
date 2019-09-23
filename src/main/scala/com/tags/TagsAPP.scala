package com.tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Row, SparkSession}


/**
  * Created by wbl 
  * on 2019/9/19 10:20
  * 广告位标签
  */
object TagsAPP extends Tag {

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val spark = SparkSession.builder().getOrCreate()

    //读取数据字典
    val docMap: collection.Map[String, String] = spark.sparkContext.textFile("")
      .map(_.split("\\s", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))
      .collectAsMap()

    //进行广播
    val broadcast = spark.sparkContext.broadcast(docMap)

    //获取数据类型
    val row: Row = args(0).asInstanceOf[Row]

    // 取媒体相关字段
    var appName = row.getAs[String]("appname")
    if (StringUtils.isBlank(appName)) {
      appName = broadcast.value.getOrElse(row.getAs[String]("appid"), "unknow")
      appName match {
        case _ => list :+= ("APP" + appName, 1)
      }
    }



    /*
    //获取广告位类型和名称
    val appName = row.getAs[String]("appname")
    //广告位类型标签
    appName match {
      case _ => list :+= ("APP"+appName,1)
    }
    */

    list

  }

}
