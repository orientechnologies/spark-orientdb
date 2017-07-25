package org.apache.spark.orientdb.documents

import com.orientechnologies.orient.core.db.record.{OTrackedList, OTrackedMap, OTrackedSet}
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.spark.orientdb.udts._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.orientdb.{QueryTest, TestUtils}
import org.apache.spark.sql.sources.{EqualTo, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class OrientDBEmbeddedUDTsSourceSuite extends QueryTest
        with BeforeAndAfterAll
        with BeforeAndAfterEach {
  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _
  private var mockOrientDBClient: OrientDBClientFactory = _
  private var expectedDataDf: DataFrame = _

  override protected def beforeAll(): Unit = {
    val conf = new SparkConf().setAppName("OrientDBEmbeddedUDTsSourceSuite")
                .setMaster("local[*]")
    sc = new SparkContext(conf)
  }

  override protected def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  override protected def beforeEach(): Unit = {
    sqlContext = new SQLContext(sc)
    mockOrientDBClient = Mockito.mock(classOf[OrientDBClientFactory],
      Mockito.RETURNS_SMART_NULLS)
    expectedDataDf = sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEmbeddedUDTs),
      TestUtils.testSchemaForEmbeddedUDTs)
  }

  override protected def afterEach(): Unit = {
    sqlContext = null
  }

  test("Can load output of OrientDB queries") {
    val query = "select embeddedset, embeddedmap from test_table"

    val querySchema = StructType(Seq(StructField("embeddedset", EmbeddedSetType),
      StructField("embeddedmap", EmbeddedMapType)))

    {
      val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root",
        "class" -> "test_table",
        "cluster" -> "test_cluster")

      val iSourceRecord = new ODocument()
      iSourceRecord.field("id", 1, OType.INTEGER)

      var oTrackedSet = new OTrackedSet[Int](iSourceRecord)
      oTrackedSet.add(1)
      var oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
      oTrackedMap.put(1, true)

      val oDoc1 = new ODocument()
      oDoc1.field("embeddedset", oTrackedSet, OType.EMBEDDEDSET)
      oDoc1.field("embeddedmap", oTrackedMap, OType.EMBEDDEDMAP)

      oTrackedSet = new OTrackedSet[Int](iSourceRecord)
      oTrackedSet.add(2)
      oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
      oTrackedMap.put(1, false)

      val oDoc2 = new ODocument()
      oDoc2.field("embeddedset", oTrackedSet, OType.EMBEDDEDSET)
      oDoc2.field("embeddedmap", oTrackedMap, OType.EMBEDDEDMAP)

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
        "cluster" -> "test_cluster")

      val iSourceRecord = new ODocument()
      iSourceRecord.field("id", 1, OType.INTEGER)

      var oTrackedSet = new OTrackedSet[Int](iSourceRecord)
      oTrackedSet.add(1)
      var oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
      oTrackedMap.put(1, true)

      val oDoc1 = new ODocument()
      oDoc1.field("embeddedset", oTrackedSet, OType.EMBEDDEDSET)
      oDoc1.field("embeddedmap", oTrackedMap, OType.EMBEDDEDMAP)

      oTrackedSet = new OTrackedSet[Int](iSourceRecord)
      oTrackedSet.add(2)
      oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
      oTrackedMap.put(1, false)

      val oDoc2 = new ODocument()
      oDoc2.field("embeddedset", oTrackedSet, OType.EMBEDDEDSET)
      oDoc2.field("embeddedmap", oTrackedMap, OType.EMBEDDEDMAP)

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
      "cluster" -> "test_cluster")

    val iSourceRecord = new ODocument()
    iSourceRecord.field("id", 1, OType.INTEGER)

    var oTrackedList = new OTrackedList[Byte](iSourceRecord)
    oTrackedList.add(1.toByte)
    var oTrackedSet = new OTrackedSet[Boolean](iSourceRecord)
    oTrackedSet.add(true)
    var oTrackedMap = new OTrackedMap[String](iSourceRecord)
    oTrackedMap.put(1, "Hello")

    val oDoc1 = new ODocument()
    oDoc1.field("embeddedlist", oTrackedList, OType.EMBEDDEDLIST)
    oDoc1.field("embeddedset", oTrackedSet, OType.EMBEDDEDSET)
    oDoc1.field("embeddedmap", oTrackedMap, OType.EMBEDDEDMAP)

    oTrackedList = new OTrackedList[Byte](iSourceRecord)
    oTrackedList.add(2.toByte)
    oTrackedSet = new OTrackedSet[Boolean](iSourceRecord)
    oTrackedSet.add(false)
    oTrackedMap = new OTrackedMap[String](iSourceRecord)
    oTrackedMap.put(1, "World")

    val oDoc2 = new ODocument()
    oDoc2.field("embeddedlist", oTrackedList, OType.EMBEDDEDLIST)
    oDoc2.field("embeddedset", oTrackedSet, OType.EMBEDDEDSET)
    oDoc2.field("embeddedmap", oTrackedMap, OType.EMBEDDEDMAP)

    val mockOrientDBDocument = new MockOrientDBDocument(Map(params("class") -> TestUtils.testSchemaForEmbeddedUDTs),
      List(oDoc1, oDoc2))

    val source = new DefaultSource(mockOrientDBDocument.documentWrapper, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForEmbeddedUDTs)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("embeddedlist", "embeddedset"), Array.empty[Filter])

    val prunedExpectedValues = Array(
      Row(EmbeddedList(Array(1.toByte)), EmbeddedSet(Array(true))), Row(EmbeddedList(Array(2.toByte)), EmbeddedSet(Array(false)))
    )

    val result = rdd.collect()
    assert(result.length === prunedExpectedValues.length)
    assert(result === prunedExpectedValues)
  }

  test("DefaultSource supports user schema, pruned and filtered scans") {
    val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
                  "user" -> "root",
                  "password" -> "root",
                  "class" -> "test_table",
                  "cluster" -> "test_cluster")

    val iSourceRecord = new ODocument()
    iSourceRecord.field("id", 1, OType.INTEGER)

    var oTrackedList = new OTrackedList[Byte](iSourceRecord)
    oTrackedList.add(1.toByte)
    var oTrackedSet = new OTrackedSet[Boolean](iSourceRecord)
    oTrackedSet.add(true)
    var oTrackedMap = new OTrackedMap[String](iSourceRecord)
    oTrackedMap.put(1, "Hello")

    val oDoc1 = new ODocument()
    oDoc1.field("embeddedlist", oTrackedList, OType.EMBEDDEDLIST)
    oDoc1.field("embeddedset", oTrackedSet, OType.EMBEDDEDSET)
    oDoc1.field("embeddedmap", oTrackedMap, OType.EMBEDDEDMAP)

    oTrackedList = new OTrackedList[Byte](iSourceRecord)
    oTrackedList.add(1.toByte)
    oTrackedSet = new OTrackedSet[Boolean](iSourceRecord)
    oTrackedSet.add(false)
    oTrackedMap = new OTrackedMap[String](iSourceRecord)
    oTrackedMap.put(1, "World")

    val oDoc2 = new ODocument()
    oDoc2.field("embeddedlist", oTrackedList, OType.EMBEDDEDLIST)
    oDoc2.field("embeddedset", oTrackedSet, OType.EMBEDDEDSET)
    oDoc2.field("embeddedmap", oTrackedMap, OType.EMBEDDEDMAP)

    val mockOrientDBDocument = new MockOrientDBDocument(Map(params("class") -> TestUtils.testSchemaForEmbeddedUDTs),
      List(oDoc1, oDoc2))

    val source = new DefaultSource(mockOrientDBDocument.documentWrapper, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForEmbeddedUDTs)

    val filters: Array[Filter] = Array(
      EqualTo("embeddedlist", oTrackedList),
      EqualTo("embeddedset", oTrackedSet)
    )

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
                .buildScan(Array("embeddedmap", "embeddedset"), filters)

    assert(rdd.collect().contains(Row(EmbeddedSet(Array(false)), EmbeddedMap(Map(1 -> "World")))))
  }
}