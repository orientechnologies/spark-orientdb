package org.apache.spark.orientdb.documents

import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameTest extends App {
  val spark = SparkSession.builder().appName("DataFrameTest").master("local[*]").getOrCreate()

  import spark.implicits._
  val df = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5)).toDF("id")

  df.write.format("org.apache.spark.orientdb.documents")
    .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
    .option("user", "root")
    .option("password", "root")
    .option("class", "test_class")
    .mode(SaveMode.Overwrite)
    .save()

  val resultDf = spark.sqlContext.read
    .format("org.apache.spark.orientdb.documents")
    .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
    .option("user", "root")
    .option("password", "root")
    .option("class", "test_class")
    .load()

  resultDf.show()
}