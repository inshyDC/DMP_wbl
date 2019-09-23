package com.location.areal


import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.util.ToSqlPro
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by wbl 
  * on 2019/9/17 16:48
  * 地域分布
  */
object AreaDistribute {

  def main(args: Array[String]): Unit = {

    if(args.length != 1){
      println("目录不正确，退出程序")
      sys.exit()
    }

    val Array(inputpath) = args

    val spark: SparkSession = SparkSession
      .builder()
      .appName("AreaDistribute")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val df: DataFrame = spark.read.parquet(inputpath)

    df.createOrReplaceTempView("log")

    val df2: DataFrame = spark.sql(
      """
        |select
        |a.provincename,
        |a.cityname,
        |a.req1,
        |a.req2,
        |a.ad_req,
        |a.rtb_req,
        |a.rtb_suc,
        |a.rtb_req / a.rtb_suc as rtb_lv,
        |a.show,
        |a.bidding,
        |a.ad_consum,
        |a.ad_cost
        |from
        |(
        |select
        |provincename,
        |cityname,
        |sum(case when requestmode = 1 and processnode >= 1 then 1 end) req1,
        |sum(case when requestmode = 1 and processnode >= 2 then 1 end) req2,
        |sum(case when requestmode = 1 and processnode = 3 then 1 end) ad_req,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 end) rtb_req,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 end) rtb_suc,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 end) show,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 end) bidding,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice / 1000 end) ad_consum,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment / 1000 end) ad_cost
        |from log
        |group by
        |provincename,
        |cityname
        |)a
      """.stripMargin)
    //df2.show()

    ToSqlPro.writeSqlTable(df2,"area")

    spark.stop()

  }
}
