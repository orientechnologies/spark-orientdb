package org.apache.spark.orientdb.graphs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.graphframes.GraphFrame

object GraphFrameTest extends App {
  val conf = new SparkConf().setAppName("MainApplication").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._
  val df = sc.parallelize(Array(1, 2, 3, 4, 5)).toDF("id")

  df.write.format("org.apache.spark.orientdb.graphs")
    .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
    .option("user", "root")
    .option("password", "root")
    .option("vertextype", "v104")
    .mode(SaveMode.Overwrite)
    .save()

  val vertices = sqlContext.read
    .format("org.apache.spark.orientdb.graphs")
    .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
    .option("user", "root")
    .option("password", "root")
    .option("vertextype", "v104")
    .load()

  var inVertex: Integer = null
  var outVertex: Integer = null
  vertices.collect().foreach(row => {
    if (inVertex == null) {
      inVertex = row.getAs[Integer]("id")
    }
    if (outVertex == null) {
      outVertex = row.getAs[Integer]("id")
    }
  })

  val df1 = sqlContext.createDataFrame(sc.parallelize(Seq(Row("friends", "1", "2"),
    Row("enemies", "2", "3"), Row("friends", "3", "1"))),
    StructType(List(StructField("relationship", StringType), StructField("src", StringType),
      StructField("dst", StringType))))

  df1.write.format("org.apache.spark.orientdb.graphs")
    .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
    .option("user", "root")
    .option("password", "root")
    .option("vertextype", "v104")
    .option("edgetype", "e104")
    .mode(SaveMode.Overwrite)
    .save()

  val edges = sqlContext.read
    .format("org.apache.spark.orientdb.graphs")
    .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
    .option("user", "root")
    .option("password", "root")
    .option("edgetype", "e104")
    .load()

  edges.show()

  val g = GraphFrame(vertices, edges)
  g.inDegrees.show()
  println(g.edges.filter("relationship = 'friends'").count())
}