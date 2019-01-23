package org.apache.spark.orientdb

import java.sql.{Date, Timestamp}
import java.util.Calendar

import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.spark.orientdb.udts._
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

  val testSchemaForVertices: StructType = {
    StructType(Seq(
      StructField("id", IntegerType),
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
      StructField("src", IntegerType),
      StructField("dst", IntegerType),
      StructField("relationship", StringType)
    ))
  }

  val testSchemaForEmbeddedUDTs: StructType = {
    StructType(Seq(
      StructField("embeddedlist", EmbeddedListType),
      StructField("embeddedset", EmbeddedSetType),
      StructField("embeddedmap", EmbeddedMapType)
    ))
  }

  val testSchemaForEmbeddedUDTsForVertices: StructType = {
    StructType(Seq(
      StructField("id", IntegerType),
      StructField("embeddedlist", EmbeddedListType),
      StructField("embeddedset", EmbeddedSetType),
      StructField("embeddedmap", EmbeddedMapType)
    ))
  }

  val testSchemaForEmbeddedUDTsForEdges: StructType = {
    StructType(Seq(
      StructField("src", IntegerType),
      StructField("dst", IntegerType),
      StructField("embeddedlist", EmbeddedListType),
      StructField("embeddedset", EmbeddedSetType),
      StructField("embeddedmap", EmbeddedMapType)
    ))
  }

  val testSchemaForLinkUDTs: StructType = {
    StructType(Seq(
      StructField("linklist", LinkListType),
      StructField("linkset", LinkSetType),
      StructField("linkmap", LinkMapType),
      StructField("linkbag", LinkBagType),
      StructField("link", LinkType)
    ))
  }

  val testSchemaForLinkUDTsForVertices: StructType = {
    StructType(Seq(
      StructField("id", IntegerType),
      StructField("linklist", LinkListType),
      StructField("linkset", LinkSetType),
      StructField("linkmap", LinkMapType),
      StructField("linkbag", LinkBagType),
      StructField("link", LinkType)
    ))
  }

  val testSchemaForLinkUDTsForEdges: StructType = {
    StructType(Seq(
      StructField("src", IntegerType),
      StructField("dst", IntegerType),
      StructField("linklist", LinkListType),
      StructField("linkset", LinkSetType),
      StructField("linkmap", LinkMapType),
      StructField("linkbag", LinkBagType),
      StructField("link", LinkType)
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

  val expectedDataForVertices: Seq[Row] = Seq(
    Row(1, 1.toByte, true, TestUtils.toDate(2015, 6, 1), 1234152.12312498,
      1.0f, 42, 1239012341823719L, 23.toShort, "Unicode's樂趣",
      TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1)),
    Row(2, 1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
      1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
    Row(3, 0.toByte, null, TestUtils.toDate(2015, 6, 3), 0.0, -1.0f, 4141214,
      1239012341823719L, null, "f", TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0)),
    Row(4, 0.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L, 24.toShort,
      "___|_123", null),
    Row(List.fill(11)(null): _*)
  )

  val expectedDataForEdges: Seq[Row] = Seq(
    Row(1, 2, "friends"),
    Row(2, 3, "enemy"),
    Row(3, 4, "friends"),
    Row(4, 1, "enemy")
  )

  val expectedDataForEmbeddedUDTs: Seq[Row] = Seq(
    Row(EmbeddedList(Array(1, 1.toByte, true, TestUtils.toDate(2015, 6, 1), 1234152.12312498,
      1.0f, 42, 1239012341823719L, 23.toShort, "Unicode's樂趣",
      TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1))),
      EmbeddedSet(Array(1, 1.toByte, true, TestUtils.toDate(2015, 6, 1), 1234152.12312498,
        1.0f, 42, 1239012341823719L, 23.toShort, "Unicode's樂趣",
        TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1))),
      EmbeddedMap(Map(1 -> 1, 2 -> 1.toByte, 3 -> true, 4 -> TestUtils.toDate(2015, 6, 1), 5 -> 1234152.12312498,
        6 -> 1.0f, 7 -> 42, 8 -> 1239012341823719L, 9 -> 23.toShort, 10 -> "Unicode's樂趣", 11 -> TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1)))),
    Row(EmbeddedList(Array(2, 1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
      1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0))),
      EmbeddedSet(Array(2, 1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
        1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0))),
      EmbeddedMap(Map(1 -> 2, 2 -> 1.toByte, 3 -> false, 4 -> TestUtils.toDate(2015, 6, 2), 5 -> 0.0, 6 -> -1.0f,
        7 -> 4141214, 8 -> 1239012341823719L, 9 -> -13.toShort, 10 -> "asdf", 11 -> TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)))),
    Row(EmbeddedList(Array(3, 0.toByte, null, TestUtils.toDate(2015, 6, 3), 0.0, -1.0f, 4141214,
      1239012341823719L, null, "f", TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0))),
      EmbeddedSet(Array(3, 0.toByte, null, TestUtils.toDate(2015, 6, 3), 0.0, -1.0f, 4141214,
        1239012341823719L, null, "f", TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0))),
      EmbeddedMap(Map(1 -> 3, 2 -> 0.toByte, 3 -> null, 4 -> TestUtils.toDate(2015, 6, 3), 5 -> 0.0, 6 -> -1.0f, 7 -> 4141214,
        8 -> 1239012341823719L, 9 -> null, 10 -> "f", 11 -> TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0)))),
    Row(EmbeddedList(Array(4, 0.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L, 24.toShort,
      "___|_123", null)),
      EmbeddedSet(Array(4, 0.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L, 24.toShort,
        "___|_123", null)),
      EmbeddedMap(Map(1 -> 4, 2 -> 0.toByte, 3 -> false, 4 -> null, 5 -> -1234152.12312498, 6 -> 100000.0f, 7 -> null,
        8 -> 1239012341823719L, 9 -> 24.toShort, 10 -> "___|_123", 11 -> null))),
    Row(EmbeddedList(Array.fill(11)(null)),
      EmbeddedSet(Array.fill(11)(null)),
      EmbeddedMap(Map.apply(Array.fill(11)(null).zipWithIndex.map{ elem =>
        (elem._2, elem._1)
      }: _*)))
  )

  val expectedDataForEmbeddedUDTsVertices: Seq[Row] = Seq(
    Row(1, EmbeddedList(Array(1, 1.toByte, true, TestUtils.toDate(2015, 6, 1), 1234152.12312498,
      1.0f, 42, 1239012341823719L, 23.toShort, "Unicode's樂趣",
      TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1))),
      EmbeddedSet(Array(1, 1.toByte, true, TestUtils.toDate(2015, 6, 1), 1234152.12312498,
        1.0f, 42, 1239012341823719L, 23.toShort, "Unicode's樂趣",
        TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1))),
      EmbeddedMap(Map(1 -> 1, 2 -> 1.toByte, 3 -> true, 4 -> TestUtils.toDate(2015, 6, 1), 5 -> 1234152.12312498,
        6 -> 1.0f, 7 -> 42, 8 -> 1239012341823719L, 9 -> 23.toShort, 10 -> "Unicode's樂趣", 11 -> TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1)))),
    Row(2, EmbeddedList(Array(2, 1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
      1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0))),
      EmbeddedSet(Array(2, 1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
        1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0))),
      EmbeddedMap(Map(1 -> 2, 2 -> 1.toByte, 3 -> false, 4 -> TestUtils.toDate(2015, 6, 2), 5 -> 0.0, 6 -> -1.0f,
        7 -> 4141214, 8 -> 1239012341823719L, 9 -> -13.toShort, 10 -> "asdf", 11 -> TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)))),
    Row(3, EmbeddedList(Array(3, 0.toByte, null, TestUtils.toDate(2015, 6, 3), 0.0, -1.0f, 4141214,
      1239012341823719L, null, "f", TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0))),
      EmbeddedSet(Array(3, 0.toByte, null, TestUtils.toDate(2015, 6, 3), 0.0, -1.0f, 4141214,
        1239012341823719L, null, "f", TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0))),
      EmbeddedMap(Map(1 -> 3, 2 -> 0.toByte, 3 -> null, 4 -> TestUtils.toDate(2015, 6, 3), 5 -> 0.0, 6 -> -1.0f, 7 -> 4141214,
        8 -> 1239012341823719L, 9 -> null, 10 -> "f", 11 -> TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0)))),
    Row(4, EmbeddedList(Array(4, 0.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L, 24.toShort,
      "___|_123", null)),
      EmbeddedSet(Array(4, 0.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L, 24.toShort,
        "___|_123", null)),
      EmbeddedMap(Map(1 -> 4, 2 -> 0.toByte, 3 -> false, 4 -> null, 5 -> -1234152.12312498, 6 -> 100000.0f, 7 -> null,
        8 -> 1239012341823719L, 9 -> 24.toShort, 10 -> "___|_123", 11 -> null))),
    Row(5, EmbeddedList(Array.fill(11)(null)),
      EmbeddedSet(Array.fill(11)(null)),
      EmbeddedMap(Map.apply(Array.fill(11)(null).zipWithIndex.map{ elem =>
        (elem._2, elem._1)
      }: _*)))
  )

  val expectedDataForEmbeddedUDTsEdges: Seq[Row] = Seq(
    Row(1, 2, EmbeddedList(Array(1, 2, "friends")),
      EmbeddedSet(Array(1, 2, "friends")),
      EmbeddedMap(Map(1 -> 1, 2 -> 2, 3 -> "friends"))),
    Row(2, 3, EmbeddedList(Array(2, 3, "enemy")),
      EmbeddedSet(Array(2, 3, "enemy")),
      EmbeddedMap(Map(1 -> 2, 2 -> 3, 3 -> "friends"))),
    Row(3, 4, EmbeddedList(Array(3, 4, "friends")),
      EmbeddedSet(Array(3, 4, "friends")),
      EmbeddedMap(Map(1 -> 3, 2 -> 4, 3 -> "friends"))),
    Row(4, 5, EmbeddedList(Array(4, 1, "enemy")),
      EmbeddedSet(Array(4, 1, "enemy")),
      EmbeddedMap(Map(1 -> 4, 2 -> 1, 3 -> "enemy")))
  )

  val oDocument1 = new ODocument("link")
  oDocument1.field("f1", 1, OType.INTEGER)
  oDocument1.field("f2", 1.toByte, OType.BYTE)
  oDocument1.field("f3", true, OType.BOOLEAN)
  oDocument1.field("f4", TestUtils.toDate(2015, 6, 1), OType.DATE)
  oDocument1.field("f5", 1234152.12312498, OType.DOUBLE)
  oDocument1.field("f6", 1.0f, OType.FLOAT)
  oDocument1.field("f7", 42, OType.INTEGER)
  oDocument1.field("f8", 1239012341823719L, OType.LONG)
  oDocument1.field("f9", 23.toShort, OType.SHORT)
  oDocument1.field("f10", "Unicode's樂趣", OType.STRING)
  oDocument1.field("f11", TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1), OType.DATETIME)

  val oRid1 = new ORecordId()
  oRid1.fromString("#1:1")

  val oDocument2 = new ODocument("link")
  oDocument2.field("f1", 2, OType.INTEGER)
  oDocument2.field("f2", 1.toByte, OType.BYTE)
  oDocument2.field("f3", false, OType.BOOLEAN)
  oDocument2.field("f4", TestUtils.toDate(2015, 6, 2), OType.DATE)
  oDocument2.field("f5", 0.0, OType.DOUBLE)
  oDocument2.field("f6", 0.0f, OType.FLOAT)
  oDocument2.field("f7", 42, OType.INTEGER)
  oDocument2.field("f8", 1239012341823719L, OType.LONG)
  oDocument2.field("f9", -13.toShort, OType.SHORT)
  oDocument2.field("f10", "asdf", OType.STRING)
  oDocument2.field("f11",  TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0), OType.DATETIME)

  val oRid2 = new ORecordId()
  oRid2.fromString("#2:2")

  val oDocument3 = new ODocument("link")
  oDocument3.field("f1", 3, OType.INTEGER)
  oDocument3.field("f2", 0.toByte, OType.BYTE)
  oDocument3.field("f3", null, OType.BOOLEAN)
  oDocument3.field("f4", TestUtils.toDate(2015, 6, 3), OType.DATE)
  oDocument3.field("f5", 0.0, OType.DOUBLE)
  oDocument3.field("f6", -1.0f, OType.FLOAT)
  oDocument3.field("f7", 4141214, OType.INTEGER)
  oDocument3.field("f8", 1239012341823719L, OType.LONG)
  oDocument3.field("f9", null, OType.SHORT)
  oDocument3.field("f10", "f", OType.STRING)
  oDocument3.field("f11",  TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0), OType.DATETIME)

  val oRid3 = new ORecordId()
  oRid3.fromString("#3:3")

  val oDocument4 = new ODocument("link")
  oDocument4.field("f1", 4, OType.INTEGER)
  oDocument4.field("f2", 0.toByte, OType.BYTE)
  oDocument4.field("f3", false, OType.BOOLEAN)
  oDocument4.field("f4", null, OType.DATE)
  oDocument4.field("f5", -1234152.12312498, OType.DOUBLE)
  oDocument4.field("f6", 100000.0f, OType.FLOAT)
  oDocument4.field("f7", null, OType.INTEGER)
  oDocument4.field("f8", 1239012341823719L, OType.LONG)
  oDocument4.field("f9", 24.toShort, OType.SHORT)
  oDocument4.field("f10", "___|_123", OType.STRING)
  oDocument4.field("f11",  null, OType.DATETIME)

  val oRid4 = new ORecordId()
  oRid4.fromString("#4:4")

  val oDocument5 = new ODocument("link")
  oDocument5.field("f1", null, OType.INTEGER)
  oDocument5.field("f2", null, OType.BYTE)
  oDocument5.field("f3", null, OType.BOOLEAN)
  oDocument5.field("f4", null, OType.DATE)
  oDocument5.field("f5", null, OType.DOUBLE)
  oDocument5.field("f6", null, OType.FLOAT)
  oDocument5.field("f7", null, OType.INTEGER)
  oDocument5.field("f8", null, OType.LONG)
  oDocument5.field("f9", null, OType.SHORT)
  oDocument5.field("f10", null, OType.STRING)
  oDocument5.field("f11", null, OType.DATETIME)

  val oRid5 = new ORecordId()
  oRid5.fromString("#5:5")

  val expectedDataForLinkUDTs: Seq[Row] = Seq(
    Row(LinkList(Array(oDocument1)), LinkSet(Array(oDocument1)), LinkMap(Map("1" -> oDocument1)), LinkBag(Array(oRid1)), Link(oDocument1)),
    Row(LinkList(Array(oDocument2)), LinkSet(Array(oDocument2)), LinkMap(Map("1" -> oDocument2)), LinkBag(Array(oRid2)), Link(oDocument2)),
    Row(LinkList(Array(oDocument3)), LinkSet(Array(oDocument3)), LinkMap(Map("1" -> oDocument3)), LinkBag(Array(oRid3)), Link(oDocument3)),
    Row(LinkList(Array(oDocument4)), LinkSet(Array(oDocument4)), LinkMap(Map("1" -> oDocument4)), LinkBag(Array(oRid4)), Link(oDocument4)),
    Row(LinkList(Array(oDocument5)), LinkSet(Array(oDocument5)), LinkMap(Map("1" -> oDocument5)), LinkBag(Array(oRid5)), Link(oDocument5))
  )

  val expectedDataForLinkUDTsForVertices: Seq[Row] = Seq(
    Row(1, LinkList(Array(oDocument1)), LinkSet(Array(oDocument1)), LinkMap(Map("1" -> oDocument1)), LinkBag(Array(oRid1)), Link(oDocument1)),
    Row(2, LinkList(Array(oDocument2)), LinkSet(Array(oDocument2)), LinkMap(Map("1" -> oDocument2)), LinkBag(Array(oRid2)), Link(oDocument2)),
    Row(3, LinkList(Array(oDocument3)), LinkSet(Array(oDocument3)), LinkMap(Map("1" -> oDocument3)), LinkBag(Array(oRid3)), Link(oDocument3)),
    Row(4, LinkList(Array(oDocument4)), LinkSet(Array(oDocument4)), LinkMap(Map("1" -> oDocument4)), LinkBag(Array(oRid4)), Link(oDocument4)),
    Row(5, LinkList(Array(oDocument5)), LinkSet(Array(oDocument5)), LinkMap(Map("1" -> oDocument5)), LinkBag(Array(oRid5)), Link(oDocument5))
  )

  val oDocument6 = new ODocument("link")
  oDocument6.field("src", 1, OType.INTEGER)
  oDocument6.field("dst", 2, OType.INTEGER)
  oDocument6.field("relationship", "friends", OType.STRING)

  val oDocument7 = new ODocument("link")
  oDocument7.field("src", 2, OType.INTEGER)
  oDocument7.field("dst", 3, OType.INTEGER)
  oDocument7.field("relationship", "enemy", OType.STRING)

  val oDocument8 = new ODocument("link")
  oDocument8.field("src", 3, OType.INTEGER)
  oDocument8.field("dst", 4, OType.INTEGER)
  oDocument8.field("relationship", "friends", OType.STRING)

  val oDocument9 = new ODocument("link")
  oDocument9.field("src", 4, OType.INTEGER)
  oDocument9.field("dst", 1, OType.INTEGER)
  oDocument9.field("relationship", "enemy", OType.STRING)

  val expectedDataForLinkUDTsForEdges: Seq[Row] = Seq(
    Row(1, 2, LinkList(Array(oDocument6)), LinkSet(Array(oDocument6)), LinkMap(Map("1" -> oDocument6)), LinkBag(Array(oRid1)), Link(oDocument6)),
    Row(2, 3, LinkList(Array(oDocument7)), LinkSet(Array(oDocument7)), LinkMap(Map("1" -> oDocument7)), LinkBag(Array(oRid2)), Link(oDocument7)),
    Row(3, 4, LinkList(Array(oDocument8)), LinkSet(Array(oDocument8)), LinkMap(Map("1" -> oDocument8)), LinkBag(Array(oRid3)), Link(oDocument8)),
    Row(4, 5, LinkList(Array(oDocument9)), LinkSet(Array(oDocument9)), LinkMap(Map("1" -> oDocument9)), LinkBag(Array(oRid4)), Link(oDocument9))
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