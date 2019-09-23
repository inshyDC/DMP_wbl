package graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * Created by wbl
  * on 2019/9/23 10:34
  * 图计算案例（好友关联推荐）
  */
object GraphTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("graph").master("local").getOrCreate()

    //创建点和边
    //构建点的集合
    val vertexRDD = spark.sparkContext.makeRDD(Seq(
      (1L, ("小明", 29)),
      (2L, ("小红", 19)),
      (6L, ("小黑", 39)),
      (9L, ("小绿", 29)),
      (133L, ("小白", 29)),
      (138L, ("小蓝", 29)),
      (158L, ("小紫", 29)),
      (16L, ("小强", 29)),
      (44L, ("小龙", 29)),
      (21L, ("小信", 29)),
      (5L, ("小点", 29)),
      (7L, ("小方", 29))
    ))

    //构造边的集合
    val edgeRDD = spark.sparkContext.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

    //构建图
    val graph = Graph(vertexRDD,edgeRDD)
    //取顶点
    val vertices = graph.connectedComponents().vertices

    vertices.join(vertexRDD).map{
      case (userId,(cnId,(name,age)))=>(cnId,List((name,age)))
    }
      .reduceByKey(_++_)
      .foreach(println)

  }
}
