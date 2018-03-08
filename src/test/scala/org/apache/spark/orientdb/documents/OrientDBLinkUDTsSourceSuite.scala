package org.apache.spark.orientdb.documents

import java.util

import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.db.record.{ORecordLazyList, ORecordLazyMap, ORecordLazySet}
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.spark.orientdb.udts._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.orientdb.{QueryTest, TestUtils}
import org.apache.spark.sql.sources.{EqualTo, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class OrientDBLinkUDTsSourceSuite extends QueryTest
        with BeforeAndAfterAll
        with BeforeAndAfterEach {
  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _
  private var mockOrientDBClient: OrientDBClientFactory = _
  private var expectedDataDf: DataFrame = _

  override protected def beforeAll(): Unit = {
    val conf = new SparkConf().setAppName("OrientDBLinkUDTsSourceSuite")
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
    expectedDataDf = sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForLinkUDTs),
      TestUtils.testSchemaForLinkUDTs)
  }

  override protected def afterEach(): Unit = {
    sqlContext = null
  }

  test("Can load output of OrientDB queries") {
    val query = "select linkset, linkmap, linkbag, link from test_link_table"

    val querySchema = StructType(Seq(StructField("linkset", LinkSetType),
      StructField("linkmap", LinkMapType),
      StructField("linkbag", LinkBagType),
      StructField("link", LinkType)))

    {
      val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root",
        "class" -> "test_link_table",
        "clusters" -> "test_link_cluster")

      val iSourceRecord = new ODocument()
      iSourceRecord.field("id", 1, OType.INTEGER)

      var oDoc1 = new ODocument()
      oDoc1.field("int", 1, OType.INTEGER)
      var oDoc2 = new ODocument()
      oDoc2.field("boolean", true)
      var oRid1 = new ORecordId()
      oRid1.fromString("#1:1")
      var oRid2 = new ORecordId()
      oRid2.fromString("#2:2")

      var oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oDoc1)
      var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oDoc2)
      var oRidBag = new ORidBag()
      oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

      val oDoc3 = new ODocument()
      oDoc3.field("linkset", oRecordLazySet, OType.LINKSET)
      oDoc3.field("linkmap", oRecordLazyMap, OType.LINKMAP)
      oDoc3.field("linkbag", oRidBag, OType.LINKBAG)
      oDoc3.field("link", oDoc1, OType.LINK)

      oDoc1 = new ODocument()
      oDoc1.field("int", 2, OType.INTEGER)
      oDoc2 = new ODocument()
      oDoc2.field("boolean", false)
      oRid1 = new ORecordId()
      oRid1.fromString("#3:3")
      oRid2 = new ORecordId()
      oRid2.fromString("#4:4")

      oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oDoc1)
      oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put(1, oDoc2)
      oRidBag = new ORidBag()
      oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

      val oDoc4 = new ODocument()
      oDoc4.field("linkset", oRecordLazySet, OType.LINKSET)
      oDoc4.field("linkmap", oRecordLazyMap, OType.LINKMAP)
      oDoc4.field("linkbag", oRidBag, OType.LINKBAG)
      oDoc4.field("link", oDoc1, OType.LINK)

      val mockOrientDBDocument = new MockOrientDBDocument(Map(params("class") -> querySchema),
        List(oDoc3, oDoc4))

      val relation = new DefaultSource(mockOrientDBDocument.documentWrapper, _ => mockOrientDBClient)
        .createRelation(sqlContext, params)
      sqlContext.baseRelationToDataFrame(relation).collect()
    }

    {
      val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root",
        "class" -> "test_link_table",
        "query" -> query,
        "clusters" -> "test_link_cluster")

      val iSourceRecord = new ODocument()
      iSourceRecord.field("id", 1, OType.INTEGER)

      var oDoc1 = new ODocument()
      oDoc1.field("int", 1, OType.INTEGER)
      var oDoc2 = new ODocument()
      oDoc2.field("boolean", true)
      var oRid1 = new ORecordId()
      oRid1.fromString("#1:1")
      var oRid2 = new ORecordId()
      oRid2.fromString("#2:2")

      var oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oDoc1)
      var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oDoc2)
      var oRidBag = new ORidBag()
      oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

      val oDoc3 = new ODocument()
      oDoc3.field("linkset", oRecordLazySet, OType.LINKSET)
      oDoc3.field("linkmap", oRecordLazyMap, OType.LINKMAP)

      oDoc1 = new ODocument()
      oDoc1.field("int", 2, OType.INTEGER)
      oDoc2 = new ODocument()
      oDoc2.field("boolean", false)
      oRid1 = new ORecordId()
      oRid1.fromString("#3:3")
      oRid2 = new ORecordId()
      oRid2.fromString("#4:4")

      oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oDoc1)
      oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put(1, oDoc2)
      oRidBag = new ORidBag()
      oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

      val oDoc4 = new ODocument()
      oDoc4.field("linkset", oRecordLazySet, OType.LINKSET)
      oDoc4.field("linkmap", oRecordLazyMap, OType.LINKMAP)
      oDoc4.field("linkbag", oRidBag, OType.LINKBAG)
      oDoc4.field("link", oDoc1, OType.LINK)

      val mockOrientDBDocument = new MockOrientDBDocument(Map(params("class") -> querySchema),
        List(oDoc3, oDoc4))

      val relation = new DefaultSource(mockOrientDBDocument.documentWrapper, _ => mockOrientDBClient)
        .createRelation(sqlContext, params)
      sqlContext.baseRelationToDataFrame(relation).collect()
    }
  }

  test("DefaultSource supports simple column filtering") {
    val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "class" -> "test_link_table",
      "clusters" -> "test_link_cluster")

    val iSourceRecord = new ODocument()
    iSourceRecord.field("id", 1, OType.INTEGER)

    var oDoc0 = new ODocument(new ORecordId("#1:1"))
    oDoc0.field("byte", 1.toByte, OType.BYTE)
    var oDoc1 = new ODocument(new ORecordId("#2:2"))
    oDoc1.field("boolean", true, OType.BOOLEAN)
    var oDoc2 = new ODocument(new ORecordId("#4:4"))
    oDoc2.field("string", "Hello")
    var oRid1 = new ORecordId()
    oRid1.fromString("#1:1")
    var oRid2 = new ORecordId()
    oRid2.fromString("#2:2")

    var oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oDoc0)
    var oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oDoc1)
    var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put("1", oDoc2)
    var oRidBag = new ORidBag()
    oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

    val oDoc3 = new ODocument()
    oDoc3.field("linklist", oRecordLazyList, OType.LINKLIST)
    oDoc3.field("linkset", oRecordLazySet, OType.LINKSET)
    oDoc3.field("linkmap", oRecordLazyMap, OType.LINKMAP)
    oDoc3.field("linkbag", oRidBag, OType.LINKBAG)
    oDoc3.field("link", oDoc1, OType.LINK)

    val expected1 = Row(LinkList(Array(oDoc0.asInstanceOf[ORecord])), LinkSet(Array(oDoc1.asInstanceOf[ORecord])))

    oDoc0 = new ODocument(new ORecordId("#1:1"))
    oDoc0.field("byte", 2.toByte, OType.BYTE)
    oDoc1 = new ODocument(new ORecordId("#2:2"))
    oDoc1.field("boolean", false, OType.BOOLEAN)
    oDoc2 = new ODocument(new ORecordId("#4:4"))
    oDoc2.field("string", "World")
    oRid1 = new ORecordId()
    oRid1.fromString("#3:3")
    oRid2 = new ORecordId()
    oRid2.fromString("#4:4")

    oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oDoc0)
    oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oDoc1)
    oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oDoc2)
    oRidBag = new ORidBag()
    oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

    val oDoc4 = new ODocument()
    oDoc4.field("linklist", oRecordLazyList, OType.LINKLIST)
    oDoc4.field("linkset", oRecordLazySet, OType.LINKSET)
    oDoc4.field("linkmap", oRecordLazyMap, OType.LINKMAP)
    oDoc4.field("linkbag", oRidBag, OType.LINKBAG)
    oDoc4.field("link", oDoc1, OType.LINK)

    val expected2 = Row(LinkList(Array(oDoc0.asInstanceOf[ORecord])), LinkSet(Array(oDoc1.asInstanceOf[ORecord])))

    val mockOrientDBDocument = new MockOrientDBDocument(Map(params("class") -> TestUtils.testSchemaForLinkUDTs),
      List(oDoc3, oDoc4))

    val source = new DefaultSource(mockOrientDBDocument.documentWrapper, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForLinkUDTs)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("linklist", "linkset"), Array.empty[Filter])

    val prunedExpectedValues = Array(expected1, expected2)

    val result = rdd.collect()
    assert(result.length === prunedExpectedValues.length)
    assert(result === prunedExpectedValues)
  }

  test("DefaultSource supports user schema, pruned and filtered scans") {
    val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "class" -> "test_link_table",
      "clusters" -> "test_link_cluster")

    val iSourceRecord = new ODocument()
    iSourceRecord.field("id", 1, OType.INTEGER)

    var oDoc0 = new ODocument(new ORecordId("#1:1"))
    oDoc0.field("byte", 1.toByte, OType.BYTE)
    var oDoc1 = new ODocument(new ORecordId("#2:2"))
    oDoc1.field("boolean", true, OType.BOOLEAN)
    var oDoc2 = new ODocument(new ORecordId("#4:4"))
    oDoc2.field("string", "Hello")
    var oRid1 = new ORecordId()
    oRid1.fromString("#1:1")
    var oRid2 = new ORecordId()
    oRid2.fromString("#2:2")

    var oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oDoc0)
    var oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oDoc1)
    var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put("1", oDoc2)
    var oRidBag = new ORidBag()
    oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

    val oDoc3 = new ODocument()
    oDoc3.field("linklist", oRecordLazyList, OType.LINKLIST)
    oDoc3.field("linkset", oRecordLazySet, OType.LINKSET)
    oDoc3.field("linkmap", oRecordLazyMap, OType.LINKMAP)
    oDoc3.field("linkbag", oRidBag, OType.LINKBAG)
    oDoc3.field("link", oDoc1, OType.LINK)

    oDoc0 = new ODocument(new ORecordId("#1:1"))
    oDoc0.field("byte", 2.toByte, OType.BYTE)
    oDoc1 = new ODocument(new ORecordId("#2:2"))
    oDoc1.field("boolean", false, OType.BOOLEAN)
    oDoc2 = new ODocument(new ORecordId("#4:4"))
    oDoc2.field("string", "World")
    oRid1 = new ORecordId()
    oRid1.fromString("#1:1")
    oRid2 = new ORecordId()
    oRid2.fromString("#2:2")

    oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oDoc0)
    oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oDoc1)
    oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oDoc2)
    oRidBag = new ORidBag()
    oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

    val oDoc4 = new ODocument()
    oDoc4.field("linklist", oRecordLazyList, OType.LINKLIST)
    oDoc4.field("linkset", oRecordLazySet, OType.LINKSET)
    oDoc4.field("linkmap", oRecordLazyMap, OType.LINKMAP)
    oDoc4.field("linkbag", oRidBag, OType.LINKBAG)
    oDoc4.field("link", oDoc1, OType.LINK)

    val mockOrientDBDocument = new MockOrientDBDocument(Map(params("class") -> TestUtils.testSchemaForLinkUDTs),
      List(oDoc3, oDoc4))

    val source = new DefaultSource(mockOrientDBDocument.documentWrapper, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForLinkUDTs)

    val filters: Array[Filter] = Array(
      EqualTo("linklist", oRecordLazyList),
      EqualTo("linkset", oRecordLazySet)
    )

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
                .buildScan(Array("linklist", "linkset", "linkbag", "link"), filters)

    assert(rdd.collect().contains(Row(LinkList(Array(oDoc0)), LinkSet(Array(oDoc1)), LinkBag(Array(oRid1, oRid2)), Link(oDoc1))))
  }
}