package com.util


import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession


/**
  * Created by wbl 
  * on 2019/9/19 10:38
  * 测试工具类
  */
object Test {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 读取数据文件
    val df = spark.read.parquet("C:\\Users\\WBL\\Desktop\\Spark用户画像分析\\dmp_parquet")
    df.map(row=>{
      AmapUtil.getBusinessFromAmap(
        String2Type.toDouble(row.getAs[String]("long")),
        String2Type.toDouble(row.getAs[String]("lat")))
    }).rdd.foreach(println)


  }
}
