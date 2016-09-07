package org.apache.spark.orientdb.documents

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class FilterPushdownSuite extends FunSuite {

  test("buildWhereClause with empty list of filters") {
    assert(FilterPushdown.buildWhereClause(StructType(Nil), Seq.empty) === "")
  }

  test("buildWhereClause with no filters that can be pushed down") {
    assert(FilterPushdown.buildWhereClause(StructType(Nil), Seq(NewFilter, NewFilter)) === "")
  }

  test("buildWhereClause with with some filters that cannot be pushed down") {
    val whereClause = FilterPushdown.buildWhereClause(testSchema, Seq(EqualTo("test_int", 1), NewFilter))
    assert(whereClause === "WHERE test_int = 1")
  }

  test("buildWhereClause with multiple filters") {
    val filters = Seq(
      EqualTo("test_bool", true),
      EqualTo("test_string", "Unicode是樂趣"),
      GreaterThan("test_double", 1000.0),
      LessThan("test_double", Double.MaxValue),
      GreaterThanOrEqual("test_float", 1.0f),
      LessThanOrEqual("test_int", 43),
      IsNotNull("test_int"),
      IsNull("test_int")
    )

    val whereClause = FilterPushdown.buildWhereClause(testSchema, filters)

    val expectedWhereClause =
      """
        |WHERE test_bool = true
        |AND test_string = 'Unicode是樂趣'
        |AND test_double > 1000.0
        |AND test_double < 1.7976931348623157E308
        |AND test_float >= 1.0
        |AND test_int <= 43
        |AND test_int IS NOT NULL
        |AND test_int IS NULL
      """.stripMargin.lines.mkString(" ").trim

    assert(whereClause === expectedWhereClause)
  }

  private val testSchema: StructType = StructType(Seq(
    StructField("test_byte", ByteType),
    StructField("test_bool", BooleanType),
    StructField("test_date", DateType),
    StructField("test_double", DoubleType),
    StructField("test_float", FloatType),
    StructField("test_int", IntegerType),
    StructField("test_long", LongType),
    StructField("test_short", ShortType),
    StructField("test_string", StringType),
    StructField("test_timestamp", TimestampType)
  ))

  private case object NewFilter extends Filter
}