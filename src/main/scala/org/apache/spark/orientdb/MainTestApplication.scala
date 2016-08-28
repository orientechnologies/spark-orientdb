package org.apache.spark.orientdb

import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object MainTestApplication extends App {
  val conf = new SparkConf().setAppName("MainTest").setMaster("local[*]")
  val session = new SparkContext(conf)
  val sqlContext = new SQLContext(session)
  val loadedDf = sqlContext.read.format("org.apache.spark.orientdb.documents")
    .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
    .option("user", "root").option("password", "root")
    .option("class", "test_orient7__").load()
  val schema = loadedDf.schema

  val toWriteDf = sqlContext.createDataFrame(session.parallelize(Seq(Row("Santosh", "Addanki"))), schema).write
    .format("org.apache.spark.orientdb.documents")
    .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
    .option("user", "root").option("password", "root")
    .option("class", "test_orient7__")
    .mode(SaveMode.Append)
    .save()

  val loadedDf1 = sqlContext.read.format("org.apache.spark.orientdb.documents")
    .option("dburl", "remote:127.0.0.1:2424/GratefulDeadConcerts")
    .option("user", "root").option("password", "root")
    .option("class", "test_orient7__").load()
  loadedDf1.foreach(println)
}