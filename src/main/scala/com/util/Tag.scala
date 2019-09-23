package com.util

/**
  * Created by wbl 
  * on 2019/9/19 10:18
  * 标签接口
  */
trait Tag {

  def makeTags(args:Any*):List[(String,Int)]

}
