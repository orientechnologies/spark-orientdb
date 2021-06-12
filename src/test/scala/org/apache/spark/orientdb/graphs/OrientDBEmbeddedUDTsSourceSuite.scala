package org.apache.spark.orientdb.graphs

import com.orientechnologies.orient.core.db.record.{OTrackedList, OTrackedMap, OTrackedSet}
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph
import org.apache.spark.orientdb.udts.{EmbeddedList, EmbeddedMapType, EmbeddedSet, EmbeddedSetType}
import org.apache.spark.SparkContext
import org.apache.spark.orientdb.{QueryTest, TestUtils}
import org.apache.spark.sql.sources.{EqualTo, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class OrientDBEmbeddedUDTsSourceSuite extends QueryTest
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
    sc = spark.sparkContext
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
      sc.parallelize(TestUtils.expectedDataForEmbeddedUDTs),
      TestUtils.testSchemaForEmbeddedUDTs)
    expectedDataDfEdges = sqlContext.createDataFrame(
      sc.parallelize(TestUtils.expectedDataForEmbeddedUDTsEdges),
      TestUtils.testSchemaForEmbeddedUDTs
    )
  }

  override protected def afterEach(): Unit = {
    sqlContext = null
  }

  test("Can load output of OrientDB Graph queries on vertices") {
    val query =
      "select embeddedset, embeddedmap from test_vertex"

    val querySchema = StructType(Seq(StructField("embeddedset", EmbeddedSetType, true),
      StructField("embeddedmap", EmbeddedMapType, true)))

    {
      val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root",
        "vertextype" -> "test_vertex")

      val oVertex1 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      val iSourceRecord = new ODocument()
      iSourceRecord.field("id", 1, OType.INTEGER)

      var oTrackedSet = new OTrackedSet[Int](iSourceRecord)
      oTrackedSet.add(1)
      var oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
      oTrackedMap.put(1, true)

      oVertex1.setProperty("id", new Integer(1))
      oVertex1.setProperty("embeddedset", oTrackedSet)
      oVertex1.setProperty("embeddedmap",oTrackedMap)

      val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      oTrackedSet = new OTrackedSet[Int](iSourceRecord)
      oTrackedSet.add(2)
      oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
      oTrackedMap.put(1, false)

      oVertex2.setProperty("id", new Integer(2))
      oVertex2.setProperty("embeddedset", oTrackedSet)
      oVertex2.setProperty("embeddedmap", oTrackedMap)

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

      var oTrackedSet = new OTrackedSet[Int](iSourceRecord)
      oTrackedSet.add(1)
      var oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
      oTrackedMap.put(1, true)

      oVertex1.setProperty("id", new Integer(1))
      oVertex1.setProperty("embeddedset", oTrackedSet)
      oVertex1.setProperty("embeddedmap",oTrackedMap)

      val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())

      oTrackedSet = new OTrackedSet[Int](iSourceRecord)
      oTrackedSet.add(2)
      oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
      oTrackedMap.put(1, false)

      oVertex2.setProperty("id", new Integer(2))
      oVertex2.setProperty("embeddedset", oTrackedSet)
      oVertex2.setProperty("embeddedmap", oTrackedMap)

      val mockOrientDBGraph = new MockOrientDBGraph(Map(params("vertextype") -> querySchema),
        List(oVertex1, oVertex2))

      val relation = new DefaultSource(mockOrientDBGraph.vertexWrapper, null, _ => mockOrientDBClient)
        .createRelation(sqlContext, params)
      sqlContext.baseRelationToDataFrame(relation).collect()
    }
  }

  test("Can load output of OrientDB Graph queries on edges") {
    val query =
      "select embeddedset, embeddedmap from test_edge"

    val querySchema = StructType(Seq(StructField("embeddedset", EmbeddedSetType, true),
      StructField("embeddedmap", EmbeddedMapType, true)))

    {
      val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root",
        "edgetype" -> "test_edge")

      val oEdge1 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())

      val iSourceRecord = new ODocument()
      iSourceRecord.field("id", 1, OType.INTEGER)

      var oTrackedSet = new OTrackedSet[Int](iSourceRecord)
      oTrackedSet.add(1)
      var oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
      oTrackedMap.put(1, true)

      oEdge1.setProperty("id", new Integer(1))
      oEdge1.setProperty("embeddedset", oTrackedSet)
      oEdge1.setProperty("embeddedmap", oTrackedMap)

      val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())

      oTrackedSet = new OTrackedSet[Int](iSourceRecord)
      oTrackedSet.add(2)
      oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
      oTrackedMap.put(1, false)

      oEdge2.setProperty("id", new Integer(2))
      oEdge2.setProperty("embeddedset", oTrackedSet)
      oEdge2.setProperty("embeddedmap", oTrackedMap)

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

      val oEdge1 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())

      val iSourceRecord = new ODocument()
      iSourceRecord.field("id", 1, OType.INTEGER)

      var oTrackedSet = new OTrackedSet[Int](iSourceRecord)
      oTrackedSet.add(1)
      var oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
      oTrackedMap.put(1, true)

      oEdge1.setProperty("id", new Integer(1))
      oEdge1.setProperty("embeddedset", oTrackedSet)
      oEdge1.setProperty("embeddedmap", oTrackedMap)

      val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())

      oTrackedSet = new OTrackedSet[Int](iSourceRecord)
      oTrackedSet.add(2)
      oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
      oTrackedMap.put(1, false)

      oEdge2.setProperty("id", new Integer(2))
      oEdge2.setProperty("embeddedset", oTrackedSet)
      oEdge2.setProperty("embeddedmap", oTrackedMap)

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

    var oTrackedList = new OTrackedList[Byte](iSourceRecord)
    oTrackedList.add(1.toByte)
    var oTrackedSet = new OTrackedSet[Int](iSourceRecord)
    oTrackedSet.add(1)
    var oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
    oTrackedMap.put(1, true)

    oVertex1.setProperty("embeddedlist", oTrackedList)
    oVertex1.setProperty("embeddedset", oTrackedSet)
    oVertex1.setProperty("embeddedmap", oTrackedMap)

    val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
      new ODocument())

    oTrackedList = new OTrackedList[Byte](iSourceRecord)
    oTrackedList.add(2.toByte)
    oTrackedSet = new OTrackedSet[Int](iSourceRecord)
    oTrackedSet.add(2)
    oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
    oTrackedMap.put(1, false)

    oVertex2.setProperty("embeddedlist", oTrackedList)
    oVertex2.setProperty("embeddedset", oTrackedSet)
    oVertex2.setProperty("embeddedmap", oTrackedMap)

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("vertextype") -> TestUtils.testSchemaForEmbeddedUDTs),
      List(oVertex1, oVertex2))

    val source = new DefaultSource(mockOrientDBGraph.vertexWrapper, null, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForEmbeddedUDTs)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
                .buildScan(Array("embeddedlist", "embeddedset"), Array.empty[Filter])

    val prunedExpectedValues = Array(
      Row(EmbeddedList(Array(1.toByte)), EmbeddedSet(Array(1))),
      Row(EmbeddedList(Array(2.toByte)), EmbeddedSet(Array(2))))

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

    var oTrackedList = new OTrackedList[Byte](iSourceRecord)
    oTrackedList.add(1.toByte)
    var oTrackedSet = new OTrackedSet[Int](iSourceRecord)
    oTrackedSet.add(1)
    var oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
    oTrackedMap.put(1, true)

    oEdge1.setProperty("embeddedlist", oTrackedList)
    oEdge1.setProperty("embeddedset", oTrackedSet)
    oEdge1.setProperty("embeddedmap", oTrackedMap)

    val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())

    oTrackedList = new OTrackedList[Byte](iSourceRecord)
    oTrackedList.add(2.toByte)
    oTrackedSet = new OTrackedSet[Int](iSourceRecord)
    oTrackedSet.add(2)
    oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
    oTrackedMap.put(1, false)

    oEdge2.setProperty("embeddedlist", oTrackedList)
    oEdge2.setProperty("embeddedset", oTrackedSet)
    oEdge2.setProperty("embeddedmap", oTrackedMap)

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("edgetype") -> TestUtils.testSchemaForEmbeddedUDTs), null,
      List(oEdge1, oEdge2))

    val source = new DefaultSource(null, mockOrientDBGraph.edgeWrapper, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForEmbeddedUDTs)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
                .buildScan(Array("embeddedset"), Array.empty[Filter])

    val prunedExpectedValues = Array(
      Row(EmbeddedSet(Array(1))), Row(EmbeddedSet(Array(2)))
    )

    assert(rdd.collect() === prunedExpectedValues)
  }

  test("DefaultSource supports user schema, pruned and filtered scans for Vertices") {
    val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "vertextype" -> "test_vertex")

    val oVertex1 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())

    val iSourceRecord = new ODocument()
    iSourceRecord.field("id", 1, OType.INTEGER)

    var oTrackedList = new OTrackedList[Byte](iSourceRecord)
    oTrackedList.add(1.toByte)
    var oTrackedSet = new OTrackedSet[Int](iSourceRecord)
    oTrackedSet.add(1)
    var oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
    oTrackedMap.put(1, true)

    oVertex1.setProperty("embeddedlist", oTrackedList)
    oVertex1.setProperty("embeddedset", oTrackedSet)
    oVertex1.setProperty("embeddedmap", oTrackedMap)

    val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())

    oTrackedList = new OTrackedList[Byte](iSourceRecord)
    oTrackedList.add(2.toByte)
    oTrackedSet = new OTrackedSet[Int](iSourceRecord)
    oTrackedSet.add(2)
    oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
    oTrackedMap.put(1, false)

    oVertex2.setProperty("embeddedlist", oTrackedList)
    oVertex2.setProperty("embeddedset", oTrackedSet)
    oVertex2.setProperty("embeddedmap", oTrackedMap)

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("vertextype") -> TestUtils.testSchemaForEmbeddedUDTs),
      List(oVertex1, oVertex2))

    val source = new DefaultSource(mockOrientDBGraph.vertexWrapper, null, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForEmbeddedUDTs)

    val filters = Array[Filter](
      EqualTo("embeddedlist", oTrackedList),
      EqualTo("embeddedset", oTrackedSet)
    )

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("embeddedlist", "embeddedset"), filters)

    assert(rdd.collect().contains(Row(EmbeddedList(Array(2.toByte)), EmbeddedSet(Array(2)))))
  }

  test("DefaultSource supports user schema, pruned and filtered scans for Edges") {
    val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "edgetype" -> "test_edge")

    val oEdge1 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())

    val iSourceRecord = new ODocument()
    iSourceRecord.field("id", 1, OType.INTEGER)

    val oTrackedList = new OTrackedList[Byte](iSourceRecord)
    oTrackedList.add(1.toByte)
    val oTrackedSet = new OTrackedSet[Int](iSourceRecord)
    oTrackedSet.add(1)
    val oTrackedMap = new OTrackedMap[Boolean](iSourceRecord)
    oTrackedMap.put(1, true)

    oEdge1.setProperty("embeddedlist", oTrackedList)
    oEdge1.setProperty("embeddedset", oTrackedSet)
    oEdge1.setProperty("embeddedmap", oTrackedMap)

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("edgetype") -> TestUtils.testSchemaForEmbeddedUDTs), null,
      List(oEdge1))

    val source = new DefaultSource(null, mockOrientDBGraph.edgeWrapper, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForEmbeddedUDTs)

    val filters = Array[Filter](
      EqualTo("embeddedlist", oTrackedList),
      EqualTo("embeddedset", oTrackedSet)
    )

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("embeddedlist", "embeddedset"), filters)

    assert(rdd.collect().contains(Row(EmbeddedList(Array(1.toByte)), EmbeddedSet(Array(1)))))
  }
}