package org.apache.spark.orientdb

import java.sql.{Date, Timestamp}
import java.util.Calendar

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object TestUtils {
  val testSchema: StructType = {
    StructType(Seq(
      StructField("testbyte", ByteType),
      StructField("testbool", BooleanType),
      StructField("testdate", DateType),
      StructField("testdouble", DoubleType),
      StructField("testfloat", FloatType),
      StructField("testint", IntegerType),
      StructField("testlong", LongType),
      StructField("testshort", ShortType),
      StructField("teststring", StringType),
      StructField("testtimestamp", TimestampType)
    ))
  }

  val testSchemaForEdges: StructType = {
    StructType(Seq(
      StructField("src", StringType),
      StructField("dst", StringType),
      StructField("relationship", StringType)
    ))
  }

  val expectedData: Seq[Row] = Seq(
    Row(1.toByte, true, TestUtils.toDate(2015, 6, 1), 1234152.12312498,
      1.0f, 42, 1239012341823719L, 23.toShort, "Unicode's樂趣",
      TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1)),
    Row(1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
      1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
    Row(0.toByte, null, TestUtils.toDate(2015, 6, 3), 0.0, -1.0f, 4141214,
      1239012341823719L, null, "f", TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0)),
    Row(0.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L, 24.toShort,
      "___|_123", null),
    Row(List.fill(10)(null): _*)
  )

  val expectedDataForEdges: Seq[Row] = Seq(
    Row()
  )

  def toMillis(
                year: Int,
                zeroBasedMonth: Int,
                date: Int,
                hour: Int,
                minutes: Int,
                seconds: Int,
                millis: Int = 0): Long = {
    val calendar = Calendar.getInstance()
    calendar.set(year, zeroBasedMonth, date, hour, minutes, seconds)
    calendar.set(Calendar.MILLISECOND, millis)
    calendar.getTime.getTime
  }

  def toTimestamp(
                   year: Int,
                   zeroBasedMonth: Int,
                   date: Int,
                   hour: Int,
                   minutes: Int,
                   seconds: Int,
                   millis: Int = 0): Timestamp = {
    new Timestamp(toMillis(year, zeroBasedMonth, date, hour, minutes, seconds, millis))
  }

  def toDate(year: Int, zeroBasedMonth: Int, date: Int): Date = {
    new Date(toTimestamp(year, zeroBasedMonth, date, 0, 0, 0).getTime)
  }
}