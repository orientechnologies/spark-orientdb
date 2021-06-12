package org.apache.spark.orientdb.graphs

import java.lang
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.db.record.{ORecordLazyList, ORecordLazyMap, ORecordLazySet}
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.record.impl.ODocument
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph
import org.apache.spark.orientdb.udts._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.orientdb.{QueryTest, TestUtils}
import org.apache.spark.sql.sources.{EqualTo, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.util

class OrientDBLinkUDTsSourceSuite extends QueryTest
  with BeforeAndAfterAll
  with BeforeAndAfterEach {
  private var sc: SparkContext = _
  private var spark: SparkSession = _
  private var sqlContext: SQLContext = _
  private var mockOrientDBClient: OrientDBClientFactory = _
  private var expectedDataDfVertices: DataFrame = _
  private var expectedDataDfEdges: DataFrame = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("OrientDBLinkUDTsSourceSuite")
      .master("local[*]")
      .getOrCreate()
    sc = spark.sparkContext;
  }

  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.close()
    }
  }

  override protected def beforeEach(): Unit = {
    sqlContext = spark.sqlContext
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
      "select linkset, linkmap, linkbag from test_vertex"

    val querySchema = StructType(Seq(StructField("linkset", LinkSetType, true),
      StructField("linkmap", LinkMapType, true),
      StructField("linkbag", LinkBagType, true)))

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
      var oRid1 = new ORecordId()
      oRid1.fromString("#1:1")
      var oRid2 = new ORecordId()
      oRid2.fromString("#2:2")

      var oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oVert1)
      var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oVert2)
      var oRidBag = new ORidBag()
      oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

      oVertex1.setProperty("linkset", oRecordLazySet)
      oVertex1.setProperty("linkmap", oRecordLazyMap)
      oVertex1.setProperty("linkbag", oRidBag)
      oVertex1.setProperty("link", oVert1)

      val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      oVert1 = new ODocument()
      oVert1.field("int", Integer.valueOf(2), OType.INTEGER)
      oVert2 = new ODocument()
      oVert2.field("boolean", new lang.Boolean(false), OType.BOOLEAN)
      oRid1 = new ORecordId()
      oRid1.fromString("#3:3")
      oRid2 = new ORecordId()
      oRid2.fromString("#4:4")

      oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oVert1)
      oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oVert2)
      oRidBag = new ORidBag()
      oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

      oVertex2.setProperty("linkset", oRecordLazySet)
      oVertex2.setProperty("linkmap", oRecordLazyMap)
      oVertex2.setProperty("linkbag", oRidBag)
      oVertex2.setProperty("link", oVert1)

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
      var oRid1 = new ORecordId()
      oRid1.fromString("#1:1")
      var oRid2 = new ORecordId()
      oRid2.fromString("#2:2")

      var oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oVert1)
      var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oVert2)
      var oRidBag = new ORidBag()
      oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

      oVertex1.setProperty("linkset", oRecordLazySet)
      oVertex1.setProperty("linkmap", oRecordLazyMap)
      oVertex1.setProperty("linkbag", oRidBag)
      oVertex1.setProperty("link", oVert1)

      val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      oVert1 = new ODocument()
      oVert1.field("int", Integer.valueOf(2), OType.INTEGER)
      oVert2 = new ODocument()
      oVert2.field("boolean", false, OType.BOOLEAN)
      oRid1 = new ORecordId()
      oRid1.fromString("#3:3")
      oRid2 = new ORecordId()
      oRid2.fromString("#4:4")

      oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oVert1)
      oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oVert2)
      oRidBag = new ORidBag()
      oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

      oVertex2.setProperty("linkset", oRecordLazySet)
      oVertex2.setProperty("linkmap", oRecordLazyMap)
      oVertex2.setProperty("linkbag", oRidBag)
      oVertex2.setProperty("link", oVert1)

      val mockOrientDBGraph = new MockOrientDBGraph(Map(params("vertextype") -> querySchema),
        List(oVertex1, oVertex2))

      val relation = new DefaultSource(mockOrientDBGraph.vertexWrapper, null, _ => mockOrientDBClient)
        .createRelation(sqlContext, params)
      sqlContext.baseRelationToDataFrame(relation).collect()
    }
  }

  test("Can load output of OrientDB Graph queries on edges") {
    val query =
      "select linkset, linkmap, linkbag from test_edge"

    val querySchema = StructType(Seq(StructField("linkset", LinkSetType, true),
      StructField("linkmap", LinkMapType, true),
      StructField("linkbag", LinkBagType, true)))

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
      var oRid1 = new ORecordId()
      oRid1.fromString("#1:1")
      var oRid2 = new ORecordId()
      oRid2.fromString("#2:2")

      var oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oEdgeA)
      var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oEdgeB)
      var oRidBag = new ORidBag()
      oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

      oEdge1.setProperty("linkset", oRecordLazySet)
      oEdge1.setProperty("linkmap", oRecordLazyMap)
      oEdge1.setProperty("linkbag", oRidBag)
      oEdge1.setProperty("link", oEdgeA)

      val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      oEdgeA = new ODocument()
      oEdgeA.field("int", Integer.valueOf(2))
      oEdgeB = new ODocument()
      oEdgeB.field("boolean", new lang.Boolean(false))
      oRid1 = new ORecordId()
      oRid1.fromString("#3:3")
      oRid2 = new ORecordId()
      oRid2.fromString("#4:4")

      oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oEdgeA)
      oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oEdgeB)
      oRidBag = new ORidBag()
      oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

      oEdge2.setProperty("linkset", oRecordLazySet)
      oEdge2.setProperty("linkmap", oRecordLazyMap)
      oEdge2.setProperty("linkbag", oRidBag)
      oEdge2.setProperty("link", oEdgeA)

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
      var oRid1 = new ORecordId()
      oRid1.fromString("#1:1")
      var oRid2 = new ORecordId()
      oRid2.fromString("#2:2")

      var oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oEdgeA)
      var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oEdgeB)
      var oRidBag = new ORidBag()
      oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

      oEdge1.setProperty("linkset", oRecordLazySet)
      oEdge1.setProperty("linkmap", oRecordLazyMap)
      oEdge1.setProperty("linkbag", oRidBag)
      oEdge1.setProperty("link", oEdgeA)

      val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      oEdgeA = new ODocument()
      oEdgeA.field("int", Integer.valueOf(2))
      oEdgeB = new ODocument()
      oEdgeB.field("boolean", new lang.Boolean(false))
      oRid1 = new ORecordId()
      oRid1.fromString("#3:3")
      oRid2 = new ORecordId()
      oRid2.fromString("#4:4")

      oRecordLazySet = new ORecordLazySet(iSourceRecord)
      oRecordLazySet.add(oEdgeA)
      oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
      oRecordLazyMap.put("1", oEdgeB)
      oRidBag = new ORidBag()
      oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

      oEdge2.setProperty("linkset", oRecordLazySet)
      oEdge2.setProperty("linkmap", oRecordLazyMap)
      oEdge2.setProperty("linkbag", oRidBag)
      oEdge2.setProperty("link", oEdgeA)

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
    var oRid1 = new ORecordId()
    oRid1.fromString("#1:1")
    var oRid2 = new ORecordId()
    oRid2.fromString("#2:2")

    var oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oVert0)
    var oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oVert1)
    var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oVert2)
    var oRidBag = new ORidBag()
    oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

    oVertex1.setProperty("linklist", oRecordLazyList)
    oVertex1.setProperty("linkset", oRecordLazySet)
    oVertex1.setProperty("linkmap", oRecordLazyMap)
    oVertex1.setProperty("linkbag", oRidBag)
    oVertex1.setProperty("link", oVert0)

    val expected1 = Row(LinkList(Array(oVert0.asInstanceOf[ORecord])), LinkSet(Array(oVert1.asInstanceOf[ORecord])), Link(oVert0))

    val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
      new ODocument())

    oVert0 = new ODocument(new ORecordId("#1:1"))
    oVert0.field("byte", 2.toByte, OType.BYTE)
    oVert1 = new ODocument(new ORecordId("#2:2"))
    oVert1.field("int", Integer.valueOf(1), OType.INTEGER)
    oVert2 = new ODocument(new ORecordId("#4:4"))
    oVert2.field("boolean", true, OType.BOOLEAN)
    oRid1 = new ORecordId()
    oRid1.fromString("#3:3")
    oRid2 = new ORecordId()
    oRid2.fromString("#4:4")

    oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oVert0)
    oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oVert1)
    oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oVert2)
    oRidBag = new ORidBag()
    oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

    oVertex2.setProperty("linklist", oRecordLazyList)
    oVertex2.setProperty("linkset", oRecordLazySet)
    oVertex2.setProperty("linkmap", oRecordLazyMap)
    oVertex2.setProperty("linkbag", oRidBag)
    oVertex2.setProperty("link", oVert0)

    val expected2 = Row(LinkList(Array(oVert0.asInstanceOf[ORecord])), LinkSet(Array(oVert1.asInstanceOf[ORecord])), Link(oVert0))

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("vertextype") -> TestUtils.testSchemaForLinkUDTs),
      List(oVertex1, oVertex2))

    val source = new DefaultSource(mockOrientDBGraph.vertexWrapper, null, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForLinkUDTs)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("linklist", "linkset", "link"), Array.empty[Filter])

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
    var oRid1 = new ORecordId()
    oRid1.fromString("#1:1")
    var oRid2 = new ORecordId()
    oRid2.fromString("#2:2")

    var oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oEdgeA)
    var oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oEdgeB)
    var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oEdgeC)
    var oRidBag = new ORidBag()
    oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

    oEdge1.setProperty("linklist", oRecordLazyList)
    oEdge1.setProperty("linkset", oRecordLazySet)
    oEdge1.setProperty("linkmap", oRecordLazyMap)
    oEdge1.setProperty("linkbag", oRidBag)
    oEdge1.setProperty("link", oEdgeA)

    val expected1 = Row(LinkList(Array(oEdgeA.asInstanceOf[ORecord])), LinkSet(Array(oEdgeB.asInstanceOf[ORecord])), Link(oEdgeA))

    val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())

    oEdgeA = new ODocument(new ORecordId("#1:1"))
    oEdgeA.field("byte", 2.toByte, OType.BYTE)
    oEdgeB = new ODocument(new ORecordId("#2:2"))
    oEdgeB.field("int", Integer.valueOf(2), OType.INTEGER)
    oEdgeC = new ODocument(new ORecordId("#4:4"))
    oEdgeC.field("boolean", false, OType.BOOLEAN)
    oRid1 = new ORecordId()
    oRid1.fromString("#3:3")
    oRid2 = new ORecordId()
    oRid2.fromString("#4:4")

    oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oEdgeA)
    oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oEdgeB)
    oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oEdgeC)
    oRidBag = new ORidBag()
    oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

    oEdge2.setProperty("linklist", oRecordLazyList)
    oEdge2.setProperty("linkset", oRecordLazySet)
    oEdge2.setProperty("linkmap", oRecordLazyMap)
    oEdge2.setProperty("linkbag", oRidBag)
    oEdge2.setProperty("link", oEdgeA)

    val expected2 = Row(LinkList(Array(oEdgeA.asInstanceOf[ORecord])), LinkSet(Array(oEdgeB.asInstanceOf[ORecord])), Link(oEdgeA))

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("edgetype") -> TestUtils.testSchemaForLinkUDTs), null,
      List(oEdge1, oEdge2))

    val source = new DefaultSource(null, mockOrientDBGraph.edgeWrapper, _ => mockOrientDBClient)

    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForLinkUDTs)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("linklist", "linkset", "link"), Array.empty[Filter])

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
    var oRid1 = new ORecordId()
    oRid1.fromString("#1:1")
    var oRid2 = new ORecordId()
    oRid2.fromString("#2:2")

    var oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oVert0)
    var oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oVert1)
    var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oVert2)
    var oRidBag = new ORidBag()
    oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

    oVertex1.setProperty("linklist", oRecordLazyList)
    oVertex1.setProperty("linkset", oRecordLazySet)
    oVertex1.setProperty("linkmap", oRecordLazyMap)
    oVertex1.setProperty("linkbag", oRidBag)
    oVertex1.setProperty("link", oVert0)

    val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
      new ODocument())

    oVert0 = new ODocument(new ORecordId("#1:1"))
    oVert0.field("byte", 2.toByte, OType.BYTE)
    oVert1 = new ODocument(new ORecordId("#2:2"))
    oVert1.field("int", Integer.valueOf(1), OType.INTEGER)
    oVert2 = new ODocument(new ORecordId("#4:4"))
    oVert2.field("boolean", true, OType.BOOLEAN)
    oRid1 = new ORecordId()
    oRid1.fromString("#3:3")
    oRid2 = new ORecordId()
    oRid2.fromString("#4:4")

    oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oVert0)
    oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oVert1)
    oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oVert2)
    oRidBag = new ORidBag()
    oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

    oVertex2.setProperty("linklist", oRecordLazyList)
    oVertex2.setProperty("linkset", oRecordLazySet)
    oVertex2.setProperty("linkmap", oRecordLazyMap)
    oVertex2.setProperty("linkbag", oRidBag)
    oVertex2.setProperty("link", oVert0)

    val expected = Row(LinkList(Array(oVert0.asInstanceOf[ORecord])), LinkSet(Array(oVert1.asInstanceOf[ORecord])),
      LinkBag(Array(oRid1, oRid2)), Link(oVert0))

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("vertextype") -> TestUtils.testSchemaForLinkUDTs),
      List(oVertex1, oVertex2))

    val source = new DefaultSource(mockOrientDBGraph.vertexWrapper, null, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForLinkUDTs)

    val filters = Array[Filter](
      EqualTo("linklist", oRecordLazyList),
      EqualTo("linkset", oRecordLazySet)
    )

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("linklist", "linkset", "linkbag", "link"), filters)

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
    var oRid1 = new ORecordId()
    oRid1.fromString("#1:1")
    var oRid2 = new ORecordId()
    oRid2.fromString("#2:2")

    var oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oEdgeA)
    var oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oEdgeB)
    var oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oEdgeC)
    var oRidBag = new ORidBag()
    oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

    oEdge1.setProperty("linklist", oRecordLazyList)
    oEdge1.setProperty("linkset", oRecordLazySet)
    oEdge1.setProperty("linkmap", oRecordLazyMap)
    oEdge1.setProperty("linkbag", oRidBag)
    oEdge1.setProperty("link", oEdgeA)

    val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())

    oEdgeA = new ODocument(new ORecordId("#1:1"))
    oEdgeA.field("byte", 2.toByte, OType.BYTE)
    oEdgeB = new ODocument(new ORecordId("#2:2"))
    oEdgeB.field("int", Integer.valueOf(2), OType.INTEGER)
    oEdgeC = new ODocument(new ORecordId("#4:4"))
    oEdgeC.field("boolean", false, OType.BOOLEAN)
    oRid1 = new ORecordId()
    oRid1.fromString("#3:3")
    oRid2 = new ORecordId()
    oRid2.fromString("#4:4")

    oRecordLazyList = new ORecordLazyList(iSourceRecord)
    oRecordLazyList.add(oEdgeA)
    oRecordLazySet = new ORecordLazySet(iSourceRecord)
    oRecordLazySet.add(oEdgeB)
    oRecordLazyMap = new ORecordLazyMap(iSourceRecord)
    oRecordLazyMap.put(1, oEdgeC)
    oRidBag = new ORidBag()
    oRidBag.addAll(util.Arrays.asList(oRid1, oRid2))

    oEdge2.setProperty("linklist", oRecordLazyList)
    oEdge2.setProperty("linkset", oRecordLazySet)
    oEdge2.setProperty("linkmap", oRecordLazyMap)
    oEdge2.setProperty("linkbag", oRidBag)
    oEdge2.setProperty("link", oEdgeA)

    val expected = Row(LinkList(Array(oEdgeA.asInstanceOf[ORecord])), LinkSet(Array(oEdgeB.asInstanceOf[ORecord])),
      LinkBag(Array(oRid1, oRid2)), Link(oEdgeA))

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("edgetype") -> TestUtils.testSchemaForLinkUDTs), null,
      List(oEdge1, oEdge2))

    val source = new DefaultSource(null, mockOrientDBGraph.edgeWrapper, _ => mockOrientDBClient)

    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForLinkUDTs)

    val filters = Array[Filter](
      EqualTo("linklist", oRecordLazyList),
      EqualTo("linkset", oRecordLazySet)
    )

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("linklist", "linkset", "linkbag", "link"), filters)

    assert(rdd.collect().contains(expected))
  }
}