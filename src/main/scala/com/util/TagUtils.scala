package com.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Created by wbl 
  * on 2019/9/19 9:54
  * 用户ID标签
  */
object TagUtils {
  //获取用户ID
  def getOneUserId(row:Row):String={
    row match {
      case t if StringUtils.isNotBlank(t.getAs[String]("imei"))=>"IM"+t.getAs[String]("imei")
      case t if StringUtils.isNotBlank(t.getAs[String]("mac"))=>"IM"+t.getAs[String]("mac")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfa"))=>"IM"+t.getAs[String]("idfa")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudid"))=>"IM"+t.getAs[String]("openudid")
      case t if StringUtils.isNotBlank(t.getAs[String]("androidid"))=>"IM"+t.getAs[String]("androidid")
      case t if StringUtils.isNotBlank(t.getAs[String]("imeimd5"))=>"IM"+t.getAs[String]("imeimd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("macmd5"))=>"IM"+t.getAs[String]("macmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfamd5"))=>"IM"+t.getAs[String]("idfamd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudidmd5"))=>"IM"+t.getAs[String]("openudidmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("androididmd5"))=>"IM"+t.getAs[String]("androididmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("imeisha1"))=>"IM"+t.getAs[String]("imeisha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("macsha1"))=>"IM"+t.getAs[String]("macsha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfasha1"))=>"IM"+t.getAs[String]("idfasha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudidsha1"))=>"IM"+t.getAs[String]("openudidsha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("androididsha1"))=>"IM"+t.getAs[String]("androididsha1")
      case _ => "其他"
    }
  }
  //获取所有用户的唯一ID
  def getAllUserId(v:Row):List[String]={
    var list = List[String]()

    if(StringUtils.isNotBlank(v.getAs[String]("imei"))) list:+="IM"+v.getAs[String]("imei")
    if(StringUtils.isNotBlank(v.getAs[String]("mac"))) list:+="MC"+v.getAs[String]("mac")
    if(StringUtils.isNotBlank(v.getAs[String]("idfa"))) list:+="ID"+v.getAs[String]("idfa")
    if(StringUtils.isNotBlank(v.getAs[String]("openudid"))) list:+="OD"+v.getAs[String]("openudid")
    if(StringUtils.isNotBlank(v.getAs[String]("androidid"))) list:+="AD"+v.getAs[String]("androidid")
    if(StringUtils.isNotBlank(v.getAs[String]("imeimd5"))) list:+="IM"+v.getAs[String]("imeimd5")
    if(StringUtils.isNotBlank(v.getAs[String]("macmd5"))) list:+="MC"+v.getAs[String]("macmd5")
    if(StringUtils.isNotBlank(v.getAs[String]("idfamd5"))) list:+="ID"+v.getAs[String]("idfamd5")
    if(StringUtils.isNotBlank(v.getAs[String]("openudidmd5"))) list:+="OD"+v.getAs[String]("openudidmd5")
    if(StringUtils.isNotBlank(v.getAs[String]("androididmd5"))) list:+="AD"+v.getAs[String]("androididmd5")
    if(StringUtils.isNotBlank(v.getAs[String]("imeisha1"))) list:+="IM"+v.getAs[String]("imeisha1")
    if(StringUtils.isNotBlank(v.getAs[String]("macsha1"))) list:+="MC"+v.getAs[String]("macsha1")
    if(StringUtils.isNotBlank(v.getAs[String]("idfasha1"))) list:+="ID"+v.getAs[String]("idfasha1")
    if(StringUtils.isNotBlank(v.getAs[String]("openudidsha1"))) list:+="OD"+v.getAs[String]("openudidsha1")
    if(StringUtils.isNotBlank(v.getAs[String]("androididsha1"))) list:+="AD"+v.getAs[String]("androididsha1")
    list
  }

}
