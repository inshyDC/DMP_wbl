package com.location.terminal

import com.util.{RptUtils, ToSqlPro}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by wbl 
  * on 2019/9/17 21:33
  * 终端设备——运营
  */
object Operate {

  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      println("目录不正确，退出程序")
      sys.exit()
    }

    val Array(inputpath,outputPath) = args

    val spark: SparkSession = SparkSession
      .builder()
      .appName("AreaDistribute")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //获取数据
    val df: DataFrame = spark.read.parquet(inputpath)

    //获取需要的字段
    df.rdd.map(row => {
      //根据指标的字段获取数据
      //requestmode	processnode	iseffective	isbilling	isbid	iswin	adorderid
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")

      //处理请求数
      val rptList: List[Double] = RptUtils.reqPt(requestmode,processnode)
      //处理展示和点击
      val clickList: List[Double] = RptUtils.clickPt(requestmode,iseffective)
      //处理竞价成功数据，广告成本等
      val adList: List[Double] = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      //所有的指标
      val allList:List[Double] = rptList ++ clickList ++ adList

      ((row.getAs[String]("ispname")),allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1 + t._2)
    }).map(t=>t._1 + "," + t._2.mkString(","))
      .saveAsTextFile(outputPath)

    spark.stop()

  }
}
