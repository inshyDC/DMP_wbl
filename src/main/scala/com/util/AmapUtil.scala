package com.util

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * Created by wbl 
  * on 2019/9/19 10:47
  * 从高德地图获取商圈信息
  */
object AmapUtil {

  //解析经纬度
  def getBusinessFromAmap(long:Double,lat:Double):String={
    //https://restapi.amap.com/v3/geocode/regeo?
    //location=116.310003,39.991957&key=59283c76b065e4ee401c2b8a4fde8f8b&extensions=all
    val location = long +","+ lat

    //获取URL
    val url = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=dbb3318958697e72f69197f0a714e739"

    //调用HTTP接口的发送请求
    val jsonStr = HttpUtil.get(url)

    //解析json串
    val jsonObject: JSONObject = JSON.parseObject(jsonStr)

    //判断当前状态是否是1
    val status: Int = jsonObject.getIntValue("status")
    if(status == 0) return ""

    //如果不为空,进一步解析字符串

    val jsonObject1 = jsonObject.getJSONObject("regeocode")
    if(jsonObject1 == null) return ""

    val jsonObject2 = jsonObject1.getJSONObject("addressComponent")
    if(jsonObject2 == null) return ""

    val jsonArray = jsonObject2.getJSONArray("businessAreas")
    if(jsonArray == null) return ""

    //定义集合取值
    val result = collection.mutable.ListBuffer[String]()

    //循环数组
    for(item <- jsonArray.toArray()){
      if(item.isInstanceOf[JSONObject]){
        val json: JSONObject = item.asInstanceOf[JSONObject]
        val name = json.getString( "name")
        result.append(name)
      }
    }
    //商圈
    result.mkString(",")
  }


}
