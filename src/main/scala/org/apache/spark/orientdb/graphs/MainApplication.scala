package org.apache.spark.orientdb.graphs

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.graphframes.GraphFrame

object MainApplication extends App {
  val conf = new SparkConf().setAppName("MainApplication").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val vertices = sqlContext.createDataFrame(List(
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30)
  )).toDF("id", "name", "age")

  val edges = sqlContext.createDataFrame(List(
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow")
  )).toDF("src", "dst", "relationship")

  val g = GraphFrame(vertices, edges)
  g.inDegrees.show()
  g.edges.filter("relationship = 'follow'").count()
}