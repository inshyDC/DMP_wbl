package com.location.media

import com.util.RptUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by wbl 
  * on 2019/9/18 10:52
  * 媒体分析指标
  */
object APP {
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      println("目录不正确，退出程序")
      sys.exit()
    }

    val Array(inputpath,outputPath,docs) = args

    val spark: SparkSession = SparkSession
      .builder()
      .appName("AreaDistribute")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //读取数据字典
    val docMap: collection.Map[String, String] = spark.sparkContext.textFile(docs)
      .map(_.split("\\s", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))
      .collectAsMap()

    //进行广播
    val broadcast: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(docMap)

    //读取数据文件
    val df: DataFrame = spark.read.parquet(inputpath)

    df.rdd.map(row => {

      // 取媒体相关字段
      var appName = row.getAs[String]("appname")
      if(StringUtils.isBlank(appName)){
        appName = broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }

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
      val rptList: List[Double] = RptUtils.reqPt(requestmode, processnode)
      //处理展示和点击
      val clickList: List[Double] = RptUtils.clickPt(requestmode, iseffective)
      //处理竞价成功数据，广告成本等
      val adList: List[Double] = RptUtils.adPt(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      //所有的指标
      val allList: List[Double] = rptList ++ clickList ++ adList

      (appName,allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1 + t._2)
    }).map(t=>t._1 + "," + t._2.mkString(","))
      .saveAsTextFile(outputPath)

    spark.stop()
  }
}
