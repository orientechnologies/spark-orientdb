package org.apache.spark.orientdb.graphs

import java.lang

import com.orientechnologies.orient.core.db.record.{ORecordLazyList, ORecordLazyMap, ORecordLazySet}
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.record.impl.ODocument
import com.tinkerpop.blueprints.impls.orient.{OrientBaseGraph, OrientEdge, OrientVertex}
import org.apache.spark.orientdb.udts.{LinkList, LinkMapType, LinkSet, LinkSetType}
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
  private var expectedDataDfVertices: DataFrame = _
  private var expectedDataDfEdges: DataFrame = _

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
    expectedDataDfVertices = sqlContext.createDataFrame(
      sc.parallelize(TestUtils.expectedDataForLinkUDTs),
      TestUtils.testSchemaForLinkUDTs)
    expectedDataDfEdges = sqlContext.createDataFrame(
      sc.parallelize(TestUtils.expectedDataForLinkUDTsForEdges),
      TestUtils.testSchemaForLinkUDTs
    )
  }

  override protected def afterEach(): Unit = {
    sqlContext = null
  }

  test("Can load output of OrientDB Graph queries on vertices") {
    val query =
      "select linkset, linkmap from test_vertex"

    val querySchema = StructType(Seq(StructField("linkset", LinkSetType, true),
      StructField("linkmap", LinkMapType, true)))

    {
      val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root",
        "vertextype" -> "test_vertex")

      val oVertex1 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      val iSourceRecord = new ODocument()
      iSourceRecord.field("id", 1, OType.INTEGER)

      var oVert1 = new ODocument()
      oVert1.field("int", Integer.valueOf(1), OType.INTEGER)
      var oVert2 = new ODocument()
      oVert2.field("boolean", new java.lang.Boolean(true), OType.BOOLEAN)

      var oRecordLazySet = new  ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oVert1)
      var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oVert2)

      oVertex1.setProperty("linkset", oRecordLazySet)
      oVertex1.setProperty("linkmap", oRecordLazyMap)

      val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      oVert1 = new ODocument()
      oVert1.field("int", Integer.valueOf(2), OType.INTEGER)
      oVert2 = new ODocument()
      oVert2.field("boolean", new lang.Boolean(false), OType.BOOLEAN)

      oRecordLazySet = new  ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oVert1)
      oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oVert2)

      oVertex2.setProperty("linkset", oRecordLazySet)
      oVertex2.setProperty("linkmap", oRecordLazyMap)

      val mockOrientDBGraph = new MockOrientDBGraph(Map(params("vertextype") -> querySchema),
        List(oVertex1, oVertex2))

      val relation = new DefaultSource(mockOrientDBGraph.vertexWrapper, null, _ => mockOrientDBClient)
        .createRelation(sqlContext, params)
      sqlContext.baseRelationToDataFrame(relation).collect()
    }

    {
      val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root",
        "vertextype" -> "test_vertex",
        "query" -> query)

      val oVertex1 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      val iSourceRecord = new ODocument()
      iSourceRecord.field("id", 1, OType.INTEGER)

      var oVert1 = new ODocument()
      oVert1.field("int", Integer.valueOf(1), OType.INTEGER)
      var oVert2 = new ODocument()
      oVert2.field("boolean", true, OType.BOOLEAN)

      var oRecordLazySet = new  ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oVert1)
      var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oVert2)

      oVertex1.setProperty("linkset", oRecordLazySet)
      oVertex1.setProperty("linkmap", oRecordLazyMap)

      val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      oVert1 = new ODocument()
      oVert1.field("int", Integer.valueOf(2), OType.INTEGER)
      oVert2 = new ODocument()
      oVert2.field("boolean", false, OType.BOOLEAN)

      oRecordLazySet = new  ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oVert1)
      oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oVert2)

      oVertex2.setProperty("linkset", oRecordLazySet)
      oVertex2.setProperty("linkmap", oRecordLazyMap)

      val mockOrientDBGraph = new MockOrientDBGraph(Map(params("vertextype") -> querySchema),
        List(oVertex1, oVertex2))

      val relation = new DefaultSource(mockOrientDBGraph.vertexWrapper, null, _ => mockOrientDBClient)
        .createRelation(sqlContext, params)
      sqlContext.baseRelationToDataFrame(relation).collect()
    }
  }

  test("Can load output of OrientDB Graph queries on edges") {
    val query =
      "select linkset, linkmap from test_edge"

    val querySchema = StructType(Seq(StructField("linkset", LinkSetType, true),
      StructField("linkmap", LinkMapType, true)))

    {
      val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root",
        "edgetype" -> "test_edge")

      val oEdge1 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      val iSourceRecord = new ODocument()
      iSourceRecord.field("id", 1, OType.INTEGER)

      var oEdgeA = new ODocument()
      oEdgeA.field("int", Integer.valueOf(1))
      var oEdgeB = new ODocument()
      oEdgeB.field("boolean", new lang.Boolean(true))

      var oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oEdgeA)
      var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oEdgeB)

      oEdge1.setProperty("linkset", oRecordLazySet)
      oEdge1.setProperty("linkmap", oRecordLazyMap)

      val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      oEdgeA = new ODocument()
      oEdgeA.field("int", Integer.valueOf(2))
      oEdgeB = new ODocument()
      oEdgeB.field("boolean", new lang.Boolean(false))

      oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oEdgeA)
      oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oEdgeB)

      oEdge2.setProperty("linkset", oRecordLazySet)
      oEdge2.setProperty("linkmap", oRecordLazyMap)

      val mockOrientDBGraph = new MockOrientDBGraph(Map(params("edgetype") -> querySchema), null,
        List(oEdge1, oEdge2))

      val relation = new DefaultSource(null, mockOrientDBGraph.edgeWrapper, _ => mockOrientDBClient)
        .createRelation(sqlContext, params)
      sqlContext.baseRelationToDataFrame(relation).collect()
    }

    {
      val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root",
        "edgetype" -> "test_edge",
        "query" -> query)

      val oEdge1 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      val iSourceRecord = new ODocument()
      iSourceRecord.field("id", 1, OType.INTEGER)

      var oEdgeA = new ODocument()
      oEdgeA.field("int", Integer.valueOf(1))
      var oEdgeB = new ODocument()
      oEdgeB.field("boolean", new lang.Boolean(true))

      var oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oEdgeA)
      var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oEdgeB)

      oEdge1.setProperty("linkset", oRecordLazySet)
      oEdge1.setProperty("linkmap", oRecordLazyMap)

      val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      oEdgeA = new ODocument()
      oEdgeA.field("int", Integer.valueOf(2))
      oEdgeB = new ODocument()
      oEdgeB.field("boolean", new lang.Boolean(false))

      oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oEdgeA)
      oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oEdgeB)

      oEdge2.setProperty("linkset", oRecordLazySet)
      oEdge2.setProperty("linkmap", oRecordLazyMap)

      val mockOrientDBGraph = new MockOrientDBGraph(Map(params("edgetype") -> querySchema), null,
        List(oEdge1, oEdge2))

      val relation = new DefaultSource(null, mockOrientDBGraph.edgeWrapper, _ => mockOrientDBClient)
        .createRelation(sqlContext, params)
      sqlContext.baseRelationToDataFrame(relation).collect()
    }
  }

  test("DefaultSource supports simple column filtering for Vertices") {
    val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "vertextype" -> "test_vertex")

    val oVertex1 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
      new ODocument())

    val iSourceRecord = new ODocument()
    iSourceRecord.field("id", 1, OType.INTEGER)

    var oVert0 = new ODocument(new ORecordId("#1:1"))
    oVert0.field("byte", 1.toByte, OType.BYTE)
    var oVert1 = new ODocument(new ORecordId("#2:2"))
    oVert1.field("int", Integer.valueOf(1), OType.INTEGER)
    var oVert2 = new ODocument(new ORecordId("#4:4"))
    oVert2.field("boolean", true, OType.BOOLEAN)

    var oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oVert0)
    var oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oVert1)
    var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oVert2)

    oVertex1.setProperty("linklist", oRecordLazyList)
    oVertex1.setProperty("linkset", oRecordLazySet)
    oVertex1.setProperty("linkmap", oRecordLazyMap)

    val expected1 = Row(LinkList(Array(oVert0.asInstanceOf[ORecord])), LinkSet(Array(oVert1.asInstanceOf[ORecord])))

    val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
      new ODocument())

    oVert0 = new ODocument(new ORecordId("#1:1"))
    oVert0.field("byte", 2.toByte, OType.BYTE)
    oVert1 = new ODocument(new ORecordId("#2:2"))
    oVert1.field("int", Integer.valueOf(1), OType.INTEGER)
    oVert2 = new ODocument(new ORecordId("#4:4"))
    oVert2.field("boolean", true, OType.BOOLEAN)

    oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oVert0)
    oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oVert1)
    oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oVert2)

    oVertex2.setProperty("linklist", oRecordLazyList)
    oVertex2.setProperty("linkset", oRecordLazySet)
    oVertex2.setProperty("linkmap", oRecordLazyMap)

    val expected2 = Row(LinkList(Array(oVert0.asInstanceOf[ORecord])), LinkSet(Array(oVert1.asInstanceOf[ORecord])))

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("vertextype") -> TestUtils.testSchemaForLinkUDTs),
      List(oVertex1, oVertex2))

    val source = new DefaultSource(mockOrientDBGraph.vertexWrapper, null, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForLinkUDTs)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("linklist", "linkset"), Array.empty[Filter])

    val prunedExpectedValues = Array(expected1, expected2)

    assert(rdd.collect() === prunedExpectedValues)
  }

  test("DefaultSource supports simple column filtering for Edges") {
    val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "edgetype" -> "test_edge")

    val oEdge1 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())

    val iSourceRecord = new ODocument()
    iSourceRecord.field("id", 1, OType.INTEGER)

    var oEdgeA = new ODocument(new ORecordId("#1:1"))
    oEdgeA.field("byte", 1.toByte, OType.BYTE)
    var oEdgeB = new ODocument(new ORecordId("#2:2"))
    oEdgeB.field("int", Integer.valueOf(1), OType.INTEGER)
    var oEdgeC = new ODocument(new ORecordId("#4:4"))
    oEdgeC.field("boolean", true, OType.BOOLEAN)

    var oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oEdgeA)
    var oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oEdgeB)
    var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oEdgeC)

    oEdge1.setProperty("linklist", oRecordLazyList)
    oEdge1.setProperty("linkset", oRecordLazySet)
    oEdge1.setProperty("linkmap", oRecordLazyMap)

    val expected1 = Row(LinkList(Array(oEdgeA.asInstanceOf[ORecord])), LinkSet(Array(oEdgeB.asInstanceOf[ORecord])))

    val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())

    oEdgeA = new ODocument(new ORecordId("#1:1"))
    oEdgeA.field("byte", 2.toByte, OType.BYTE)
    oEdgeB = new ODocument(new ORecordId("#2:2"))
    oEdgeB.field("int", Integer.valueOf(2), OType.INTEGER)
    oEdgeC = new ODocument(new ORecordId("#4:4"))
    oEdgeC.field("boolean", false, OType.BOOLEAN)

    oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oEdgeA)
    oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oEdgeB)
    oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oEdgeC)

    oEdge2.setProperty("linklist", oRecordLazyList)
    oEdge2.setProperty("linkset", oRecordLazySet)
    oEdge2.setProperty("linkmap", oRecordLazyMap)

    val expected2 = Row(LinkList(Array(oEdgeA.asInstanceOf[ORecord])), LinkSet(Array(oEdgeB.asInstanceOf[ORecord])))

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("edgetype") -> TestUtils.testSchemaForLinkUDTs), null,
      List(oEdge1, oEdge2))

    val source = new DefaultSource(null, mockOrientDBGraph.edgeWrapper, _ => mockOrientDBClient)

    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForLinkUDTs)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("linklist", "linkset"), Array.empty[Filter])

    val prunedExpectedValues = Array(expected1, expected2)

    assert(rdd.collect() === prunedExpectedValues)
  }

  test("DefaultSource supports user schema, pruned and filtered scans for Vertices") {
    val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "vertextype" -> "test_vertex")

    val oVertex1 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
      new ODocument())

    val iSourceRecord = new ODocument()
    iSourceRecord.field("id", 1, OType.INTEGER)

    var oVert0 = new ODocument(new ORecordId("#1:1"))
    oVert0.field("byte", 1.toByte, OType.BYTE)
    var oVert1 = new ODocument(new ORecordId("#2:2"))
    oVert1.field("int", Integer.valueOf(1), OType.INTEGER)
    var oVert2 = new ODocument(new ORecordId("#4:4"))
    oVert2.field("boolean", true, OType.BOOLEAN)

    var oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oVert0)
    var oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oVert1)
    var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oVert2)

    oVertex1.setProperty("linklist", oRecordLazyList)
    oVertex1.setProperty("linkset", oRecordLazySet)
    oVertex1.setProperty("linkmap", oRecordLazyMap)

    val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
      new ODocument())

    oVert0 = new ODocument(new ORecordId("#1:1"))
    oVert0.field("byte", 2.toByte, OType.BYTE)
    oVert1 = new ODocument(new ORecordId("#2:2"))
    oVert1.field("int", Integer.valueOf(1), OType.INTEGER)
    oVert2 = new ODocument(new ORecordId("#4:4"))
    oVert2.field("boolean", true, OType.BOOLEAN)

    oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oVert0)
    oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oVert1)
    oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oVert2)

    oVertex2.setProperty("linklist", oRecordLazyList)
    oVertex2.setProperty("linkset", oRecordLazySet)
    oVertex2.setProperty("linkmap", oRecordLazyMap)

    val expected = Row(LinkList(Array(oVert0.asInstanceOf[ORecord])), LinkSet(Array(oVert1.asInstanceOf[ORecord])))

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("vertextype") -> TestUtils.testSchemaForLinkUDTs),
      List(oVertex1, oVertex2))

    val source = new DefaultSource(mockOrientDBGraph.vertexWrapper, null, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForLinkUDTs)

    val filters = Array[Filter](
      EqualTo("linklist", oRecordLazyList),
      EqualTo("linkset", oRecordLazySet)
    )

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("linklist", "linkset"), filters)

    assert(rdd.collect().contains(expected))
  }

  test("DefaultSource supports user schema, pruned and filtered scans for Edges") {
    val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "edgetype" -> "test_edge")

    val oEdge1 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())

    val iSourceRecord = new ODocument()
    iSourceRecord.field("id", 1, OType.INTEGER)

    var oEdgeA = new ODocument(new ORecordId("#1:1"))
    oEdgeA.field("byte", 1.toByte, OType.BYTE)
    var oEdgeB = new ODocument(new ORecordId("#2:2"))
    oEdgeB.field("int", Integer.valueOf(1), OType.INTEGER)
    var oEdgeC = new ODocument(new ORecordId("#4:4"))
    oEdgeC.field("boolean", true, OType.BOOLEAN)

    var oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oEdgeA)
    var oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oEdgeB)
    var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oEdgeC)

    oEdge1.setProperty("linklist", oRecordLazyList)
    oEdge1.setProperty("linkset", oRecordLazySet)
    oEdge1.setProperty("linkmap", oRecordLazyMap)

    val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())

    oEdgeA = new ODocument(new ORecordId("#1:1"))
    oEdgeA.field("byte", 2.toByte, OType.BYTE)
    oEdgeB = new ODocument(new ORecordId("#2:2"))
    oEdgeB.field("int", Integer.valueOf(2), OType.INTEGER)
    oEdgeC = new ODocument(new ORecordId("#4:4"))
    oEdgeC.field("boolean", false, OType.BOOLEAN)

    oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oEdgeA)
    oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oEdgeB)
    oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oEdgeC)

    oEdge2.setProperty("linklist", oRecordLazyList)
    oEdge2.setProperty("linkset", oRecordLazySet)
    oEdge2.setProperty("linkmap", oRecordLazyMap)

    val expected = Row(LinkList(Array(oEdgeA.asInstanceOf[ORecord])), LinkSet(Array(oEdgeB.asInstanceOf[ORecord])))

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("edgetype") -> TestUtils.testSchemaForLinkUDTs), null,
      List(oEdge1, oEdge2))

    val source = new DefaultSource(null, mockOrientDBGraph.edgeWrapper, _ => mockOrientDBClient)

    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForLinkUDTs)

    val filters = Array[Filter](
      EqualTo("linklist", oRecordLazyList),
      EqualTo("linkset", oRecordLazySet)
    )

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("linklist", "linkset"), filters)

    assert(rdd.collect().contains(expected))
  }
}