package org.apache.spark.orientdb.documents

import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.orientdb.{QueryTest, TestUtils}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{EqualTo, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{BooleanType, ByteType, StructField, StructType}
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class OrientDBSourceSuite extends QueryTest
        with BeforeAndAfterAll
        with BeforeAndAfterEach {
  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _
  private var mockOrientDBClient: OrientDBClientFactory = _
  private var expectedDataDf: DataFrame = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf().setAppName("OrientDBSourceSuite")
      .setMaster("local[*]")
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (sc != null)
      sc.stop()
  }

  override def beforeEach(): Unit = {
    sqlContext = new SQLContext(sc)
    mockOrientDBClient = Mockito.mock(classOf[OrientDBClientFactory],
      Mockito.RETURNS_SMART_NULLS)
    expectedDataDf = sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData),
      TestUtils.testSchema)
  }

  override def afterEach(): Unit = {
    sqlContext = null
  }

  test("Can load output of OrientDB queries") {
    val query =
      """select testbyte, testbool from test_table where teststring = '\\Unicode''s樂趣'"""

    val querySchema = StructType(Seq(StructField("testbyte", ByteType),
      StructField("testbool", BooleanType)))

    {
      val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root",
        "class" -> "test_table",
        "clusters" -> "test_cluster")

      val oDoc1 = new ODocument()
      oDoc1.field("testbyte", 1, OType.BYTE)
      oDoc1.field("testbool", true, OType.BOOLEAN)

      val oDoc2 = new ODocument()
      oDoc2.field("testbyte", 2, OType.BYTE)
      oDoc2.field("testbool", false, OType.BOOLEAN)

      val mockOrientDBDocument = new MockOrientDBDocument(Map(params("class") -> querySchema),
        List(oDoc1, oDoc2))

      val relation = new DefaultSource(mockOrientDBDocument.documentWrapper, _ => mockOrientDBClient)
        .createRelation(sqlContext, params)
      sqlContext.baseRelationToDataFrame(relation).collect()
    }

    {
      val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root",
        "class" -> "test_table",
        "query" -> query,
        "clusters" -> "test_cluster")

      val oDoc1 = new ODocument()
      oDoc1.field("testbyte", 1, OType.BYTE)
      oDoc1.field("testbool", true, OType.BOOLEAN)

      val oDoc2 = new ODocument()
      oDoc2.field("testbyte", 2, OType.BYTE)
      oDoc2.field("testbool", false, OType.BOOLEAN)

      val mockOrientDBDocument = new MockOrientDBDocument(Map(params("class") -> querySchema),
        List(oDoc1, oDoc2))

      val relation = new DefaultSource(mockOrientDBDocument.documentWrapper, _ => mockOrientDBClient)
        .createRelation(sqlContext, params)
      sqlContext.baseRelationToDataFrame(relation).collect()
    }
  }

  test("DefaultSource supports simple column filtering") {
    val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "class" -> "test_table",
      "clusters" -> "test_cluster")

    val oDoc1 = new ODocument()
    oDoc1.field("testbyte", 1, OType.BYTE)
    oDoc1.field("testbool", true, OType.BOOLEAN)
    oDoc1.field("teststring", "Hello", OType.STRING)

    val oDoc2 = new ODocument()
    oDoc2.field("testbyte", 2, OType.BYTE)
    oDoc2.field("testbool", false, OType.BOOLEAN)
    oDoc2.field("teststring", "World", OType.STRING)

    val mockOrientDBDocument = new MockOrientDBDocument(Map(params("class") -> TestUtils.testSchema),
      List(oDoc1, oDoc2))

    val source = new DefaultSource(mockOrientDBDocument.documentWrapper, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchema)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), Array.empty[Filter])

    val prunedExpectedValues = Array(
      Row(1.toByte, true),
      Row(2.toByte, false)
    )
    assert(rdd.collect() === prunedExpectedValues)
  }

  test("DefaultSource supports user schema, pruned and filtered scans") {
    val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "class" -> "test_table",
      "clusters" -> "test_cluster")

    val oDoc1 = new ODocument()
    oDoc1.field("testbyte", 1, OType.BYTE)
    oDoc1.field("testbool", true, OType.BOOLEAN)
    oDoc1.field("teststring", "Hello", OType.STRING)

    val oDoc2 = new ODocument()
    oDoc2.field("testbyte", 2, OType.BYTE)
    oDoc2.field("testbool", false, OType.BOOLEAN)
    oDoc2.field("teststring", "World", OType.STRING)

    val mockOrientDBDocument = new MockOrientDBDocument(Map(params("class") -> TestUtils.testSchema),
      List(oDoc1, oDoc2))

    val source = new DefaultSource(mockOrientDBDocument.documentWrapper, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchema)

    val filters: Array[Filter] = Array(
      EqualTo("testbool", true),
      EqualTo("teststring", "Hello")
    )

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), filters)

    assert(rdd.collect().contains(Row(1, true)))
  }

  test("Cannot save when 'query' parameter is specified instead of 'class'") {
    val invalidParams = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "query" -> "select testbyte, testbool from test_table where teststring = '\\Unicode''s樂趣'",
      "clusters" -> "test_cluster"
    )

    intercept[IllegalArgumentException] {
      expectedDataDf.write.format("org.apache.spark.orientdb.documents").options(invalidParams).save()
    }
  }

  test("DefaultSource has default constructor, required by Data Source API") {
    new DefaultSource()
  }
}