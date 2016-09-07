package org.apache.spark.orientdb.documents

import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class ConversionsSuite extends FunSuite {

  test("Spark datatype to OrientDB datatype test") {
    val orientDBType = Conversions.sparkDTtoOrientDBDT(StringType)
    assert(orientDBType === OType.STRING)
  }

  test("Convert Spark Row to Orient DB ODocument") {
    val expectedData = new ODocument()
    expectedData.field("key", 1, OType.INTEGER)
    expectedData.field("value", "Spark datasource for Orient DB", OType.STRING)

    val conf = new SparkConf().setAppName("ConversionsSuite").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val rows = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1, "Spark datasource for Orient DB"))),
      StructType(Array(StructField("key", IntegerType, true),
        StructField("value", StringType, true)))).collect()

    val actualData = Conversions.convertRowsToODocuments(rows(0))
    assert(expectedData.field[Int]("key") == actualData.field[Int]("key"))
    assert(expectedData.field[String]("value") == actualData.field[String]("value"))
    sc.stop()
  }

  test("Convert OrientDB ODocument to Spark Row") {
    val oDocument = new ODocument()
    oDocument.field("key", 1, OType.INTEGER)
    oDocument.field("value", "Orient DB ODocument to Spark Row", OType.STRING)

    val schema = StructType(Array(StructField("key", IntegerType),
      StructField("value", StringType)))

    val expectedData = Row(1, "Orient DB ODocument to Spark Row")
    val actualData = Conversions.convertODocumentsToRows(oDocument, schema)

    assert(expectedData === actualData)
  }

  test("Return field of correct type") {
    val field = Conversions.orientDBDTtoSparkDT(IntegerType, "1")
    assert(field.isInstanceOf[Int])
  }
}