package org.apache.spark.orientdb.documents

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameTest extends App {
  val conf = new SparkConf().setAppName("DataFrameTest").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._
  val df = sc.parallelize(Array(1, 2, 3, 4, 5)).toDF("id")

  df.write.format("org.apache.spark.orientdb.documents")
    .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
    .option("user", "root")
    .option("password", "root")
    .option("class", "test_class")
    .mode(SaveMode.Overwrite)
    .save()

  val resultDf = sqlContext.read
    .format("org.apache.spark.orientdb.documents")
    .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
    .option("user", "root")
    .option("password", "root")
    .option("class", "test_class")
    .load()

  resultDf.show()
}