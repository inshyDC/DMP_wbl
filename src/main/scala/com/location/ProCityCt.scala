package com.location

import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.util.ToSqlPro
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by wbl
  * on 2019/9/17 14:18
  * 统计省市指标
  */
object ProCityCt {

  def main(args: Array[String]): Unit = {

    if(args.length != 1){
      println("目录不正确，退出程序")
      sys.exit()
    }

    val  Array(inputPath) = args

    val spark: SparkSession = SparkSession
      .builder()
      .appName("ct")
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //spark.conf("spark.sql.parquet.compression.codec", "snappy")

    val df: DataFrame = spark.read.parquet(inputPath)

    df.createOrReplaceTempView("log")

    //sql风格
    val df2 = spark.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")

    //dsl风格
    //val ds = df.selectExpr("provincename,cityname,count(*) ct").groupBy("provincename,cityname")
    //println(ds.count())

    /*
     //存储到json文件中，且根据省市创建多级目录

    df2.write.partitionBy("provincename","cityname").json("C:\\Users\\WBL\\Desktop\\procity")
    */

    ToSqlPro.writeSqlTable(df2 , "procity")

    spark.stop()

  }

}
