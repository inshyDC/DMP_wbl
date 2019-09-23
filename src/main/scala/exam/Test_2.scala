package exam

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.{ SparkSession}

/**
  * Created by wbl 
  * on 2019/9/22 10:55
  *
  */
object Test_2 {
  def main(args: Array[String]): Unit = {

    if(args.length != 1){
      println("目录不正确，退出程序")
      sys.exit()
    }

    val  Array(inputPath) = args

    val spark: SparkSession = SparkSession
      .builder()
      .appName("test1")
      .master("local[*]")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile(inputPath)

    rdd.map(row=>{
      //解析json串
      val jsonObject = JSON.parseObject(row)
      val status: Int = jsonObject.getIntValue("status")
      if(status == 0) return ""

      val jsonArray = jsonObject.getJSONArray("pois")
      if(jsonArray == null) return ""

      //定义集合取值
      var list = List[(String,Int)]()

      /*for(item <- jsonArray.toArray()){
        if(item.isInstanceOf[JSONObject]){
          val json: JSONObject = item.asInstanceOf[JSONObject]
          val  t = json.getString( "type")
          val tName = t.split(";")
          list :+= (tName,1)
        }
      }*/

      //list.reduceLeft(_._2)

    })

  }
}
