package com.util

/**
  * Created by wbl 
  * on 2019/9/17 10:33
  * 类型转换工具类
  */
object String2Type {

  def toInt(str:String):Int={
    try{
      str.toInt
    }catch {
      case _ :Exception =>0
    }
  }

  def toDouble(str: String):Double ={
    try{
      str.toDouble
    }catch {
      case _ :Exception =>0.0
    }
  }
}
