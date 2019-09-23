package com.util

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/**
  * Created by wbl 
  * on 2019/9/19 10:31
  * Http请求协议
  */
object HttpUtil {

  //GET请求
  def get(url:String):String={
    val client: CloseableHttpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    //发送请求
    val httpResponse: CloseableHttpResponse = client.execute(httpGet)
    //处理乱码问题
    EntityUtils.toString(httpResponse.getEntity,"UTF-8")
  }

}
