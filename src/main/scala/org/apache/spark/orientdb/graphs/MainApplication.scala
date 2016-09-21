package org.apache.spark.orientdb.graphs

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.graphframes.GraphFrame

object MainApplication extends App {
  val conf = new SparkConf().setAppName("MainApplication").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._
  val df = sc.parallelize(Array(1, 2, 3, 4, 5)).toDF("id1")

  df.write.format("org.apache.spark.orientdb.graphs")
    .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
    .option("user", "root")
    .option("password", "root")
    .option("vertextype", "v100")
    .mode(SaveMode.Overwrite)
    .save()

  val vertices = sqlContext.read
                .format("org.apache.spark.orientdb.graphs")
                .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
                .option("user", "root")
                .option("password", "root")
                .option("vertextype", "v100")
                .load()

  var inVertex: String = null
  var outVertex: String = null
  vertices.collect().foreach(row => {
    if (inVertex != null) {
      inVertex = row.getAs[String]("id")
    }
    if (outVertex != null) {
      outVertex = row.getAs[String]("id")
    }
  })

  val df1 = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1, inVertex, outVertex))),
    StructType(List(StructField("id1", IntegerType), StructField("src", StringType),
      StructField("dst", StringType))))

  df1.write.format("org.apache.spark.orientdb.graphs")
    .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
    .option("user", "root")
    .option("password", "root")
    .option("edgetype", "e100")
    .mode(SaveMode.Overwrite)
    .save()

  val edges = sqlContext.read
              .format("org.apache.spark.orientdb.graphs")
              .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
              .option("user", "root")
              .option("password", "root")
              .option("edgetype", "e100")
              .load()

  edges.show()

  val g = GraphFrame(vertices, edges)
  g.inDegrees.show()
  println(g.edges.filter("id1 = 1").count())
}