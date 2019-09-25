package com.tags

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, String2Type, Tag, ToSqlPro}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}


/**
  * Created by wbl 
  * on 2019/9/19 11:25
  * 商圈标签
  */
object BusinessTag extends Tag{

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    //获取数据
    val row = args(0).asInstanceOf[Row]

    //获取经纬度
    if(String2Type.toDouble(row.getAs[String]("long")) >= 73
      && String2Type.toDouble(row.getAs[String]("long")) <= 136
      && String2Type.toDouble(row.getAs[String]("lat")) >= 3
      && String2Type.toDouble(row.getAs[String]("lat")) <= 53)
    {
      //经纬度
      val long = row.getAs[String]("long").toDouble
      val lat = row.getAs[String]("lat").toDouble

      //获取商圈名称
      val business = getBusiness(long,lat)
      if(StringUtils.isNoneBlank(business)){
        val str = business.split(",")
        str.foreach(str=>{
          list :+=(str,1)
        })
      }
    }
    list
  }

  //数据库获取商圈信息
  def redis_queryBusiness(geohash: String): String = {
    val spark = SparkSession.builder().getOrCreate()
    val df = ToSqlPro.readSqlTable(spark,geohash)
    val business = df.toString()
    spark.close()
    business

    /*val jedis = JedisConnectionPool.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business*/


  }

  def redis_insertBusiness(geohash: String, business: String) = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val rdd: RDD[(String,String)] = spark.sparkContext.makeRDD(Array((geohash,business)))

    val res: RDD[businessStr] = rdd.map {

      case (geohash, business) =>
        businessStr(geohash, business)
    }

    ToSqlPro.writeSqlTable(res.toDF(),"geohash_business")
    spark.close()


    /*val jedis = JedisConnectionPool.getConnection()
    jedis.set(geohash,business)
    jedis.close()*/


  }

  //获取商圈的信息
  def getBusiness(long: Double, lat: Double):String={
    //GeoHash码
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)

    //数据库查询当前的商圈信息
    //var business = redis_queryBusiness(geohash)

    //if(business == null){
      //去高德请求
      val business = AmapUtil.getBusinessFromAmap(long,lat)
      //将获取的商圈存储到数据库中
        /*
        if(business != null && business.length > 0){
          redis_insertBusiness(geohash,business)
        }
        */
    //}
    business
  }

}

case class businessStr(geohash:String,business:String)
