package com.util

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by wbl 
  * on 2019/9/17 21:39
  * 将数据写入数据库
  */
object ToSqlPro {

  //通过config配置文件依赖进行加载相关的配置信息
  val load = ConfigFactory.load()
  // 创建Properties对象
  val prop = new Properties()
  prop.setProperty("user",load.getString("jdbc.user"))
  prop.setProperty("password",load.getString("jdbc.password"))

  def writeSqlTable(df:DataFrame ,str:String): Unit = {
    // 存储
    df.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),str,prop)
  }

  def readSqlTable(spark:SparkSession, str:String): DataFrame = {
    // 读取
    spark.read.jdbc(load.getString("jdbc.url"),str,prop)

  }

}
