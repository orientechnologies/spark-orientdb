package org.apache.spark.orientdb.graphs

import com.orientechnologies.orient.core.record.impl.ODocument
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.orientdb.{QueryTest, TestUtils}
import org.apache.spark.sql.sources.{EqualTo, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class OrientDBGraphSourceSuite extends QueryTest
              with BeforeAndAfterAll
              with BeforeAndAfterEach {
  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _
  private var mockOrientDBClient: OrientDBClientFactory = _
  private var expectedDataDfVertices: DataFrame = _
  private var expectedDataDfEdges: DataFrame = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf().setAppName("OrientDBSourceSuite")
                  .setMaster("local[*]")
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  override def beforeEach(): Unit = {
    sqlContext = new SQLContext(sc)
    mockOrientDBClient = Mockito.mock(classOf[OrientDBClientFactory],
      Mockito.RETURNS_SMART_NULLS)
    expectedDataDfVertices = sqlContext.createDataFrame(
      sc.parallelize(TestUtils.expectedDataForVertices),
      TestUtils.testSchemaForVertices)
    expectedDataDfEdges = sqlContext.createDataFrame(
      sc.parallelize(TestUtils.expectedDataForEdges),
      TestUtils.testSchemaForEdges)
  }

  override def afterEach(): Unit = {
    sqlContext = null
  }

  test("Can load output of OrientDB Graph queries on vertices") {
    val query =
      "select testbyte, testbool from test_vertex where teststring = '\\Unicode''s樂趣'"

    val querySchema = StructType(Seq(StructField("testbyte", ByteType, true),
      StructField("testbool", BooleanType, true)))

    {
      val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root",
        "vertextype" -> "test_vertex")

      val oVertex1 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())
      oVertex1.setProperty("id", new Integer(1))
      oVertex1.setProperty("testbyte", new java.lang.Byte(1.toByte))
      oVertex1.setProperty("testbool", new java.lang.Boolean(true))

      val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())
      oVertex2.setProperty("id", new Integer(2))
      oVertex2.setProperty("testbyte", new java.lang.Byte(2.toByte))
      oVertex2.setProperty("testbool", new java.lang.Boolean(false))

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
      oVertex1.setProperty("id", new Integer(1))
      oVertex1.setProperty("testbyte", new java.lang.Byte(1.toByte))
      oVertex1.setProperty("testbool", new java.lang.Boolean(true))

      val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
        new ODocument())
      oVertex2.setProperty("id", new Integer(2))
      oVertex2.setProperty("testbyte", new java.lang.Byte(2.toByte))
      oVertex2.setProperty("testbool", new java.lang.Boolean(false))

      val mockOrientDBGraph = new MockOrientDBGraph(Map(params("vertextype") -> querySchema),
        List(oVertex1, oVertex2))

      val relation = new DefaultSource(mockOrientDBGraph.vertexWrapper, null, _ => mockOrientDBClient)
        .createRelation(sqlContext, params)
      sqlContext.baseRelationToDataFrame(relation).collect()
    }
  }

  test("Can load output of OrientDB Graph queries on edges") {
    val query =
      "select relationship from test_edge where relationship = 'enemy'"

    val querySchema = StructType(Seq(StructField("relationship", StringType)))

    {
      val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root",
        "edgetype" -> "test_edge")

      val oEdge1 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())
      oEdge1.setProperty("src", new Integer(1))
      oEdge1.setProperty("dst", new Integer(2))
      oEdge1.setProperty("relationship", new String("enemy"))

      val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())
      oEdge2.setProperty("src", new Integer(1))
      oEdge2.setProperty("dst", new Integer(2))
      oEdge2.setProperty("relationship", new String("friend"))

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
      oEdge1.setProperty("src", new Integer(1))
      oEdge1.setProperty("dst", new Integer(2))
      oEdge1.setProperty("relationship", new String("enemy"))

      val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())
      oEdge2.setProperty("src", new Integer(1))
      oEdge2.setProperty("dst", new Integer(2))
      oEdge2.setProperty("relationship", new String("friend"))

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
    oVertex1.setProperty("id", new Integer(1))
    oVertex1.setProperty("testbyte", new java.lang.Byte(1.toByte))
    oVertex1.setProperty("testbool", new java.lang.Boolean(true))

    val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
      new ODocument())
    oVertex2.setProperty("id", new Integer(2))
    oVertex2.setProperty("testbyte", new java.lang.Byte(2.toByte))
    oVertex2.setProperty("testbool", new java.lang.Boolean(false))

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("vertextype") -> TestUtils.testSchemaForVertices),
      List(oVertex1, oVertex2))

    val source = new DefaultSource(mockOrientDBGraph.vertexWrapper, null, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForVertices)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
                .buildScan(Array("testbyte", "testbool"), Array.empty[Filter])

    val prunedExpectedValues = Array(
      Row(1.toByte, true),
      Row(2.toByte, false)
    )
    assert(rdd.collect() === prunedExpectedValues)
  }

  test("DefaultSource supports simple column filtering for Edges") {
    val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "edgetype" -> "test_edge")

    val oEdge1 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())
    oEdge1.setProperty("src", new Integer(1))
    oEdge1.setProperty("dst", new Integer(2))
    oEdge1.setProperty("relationship", new String("enemy"))

    val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())
    oEdge2.setProperty("src", new Integer(1))
    oEdge2.setProperty("dst", new Integer(2))
    oEdge2.setProperty("relationship", new String("friend"))

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("edgetype") -> TestUtils.testSchemaForEdges),
      null, List(oEdge1, oEdge2))

    val source = new DefaultSource(null, mockOrientDBGraph.edgeWrapper, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForEdges)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
                .buildScan(Array("relationship"), Array.empty[Filter])

    val prunedExpectedValues = Array(
      Row("enemy"), Row("friend")
    )
    assert(rdd.collect() === prunedExpectedValues)
  }

  test("DefaultSource supports user schema, pruned and filtered scans for Vertices") {
    val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "vertextype" -> "test_vertex")

    val oVertex1 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
      new ODocument())
    oVertex1.setProperty("id", new Integer(1))
    oVertex1.setProperty("testbyte", new java.lang.Byte(1.toByte))
    oVertex1.setProperty("testbool", new java.lang.Boolean(true))

    val oVertex2 = new MockVertex(Mockito.mock(classOf[OrientBaseGraph]),
      new ODocument())
    oVertex2.setProperty("id", new Integer(2))
    oVertex2.setProperty("testbyte", new java.lang.Byte(2.toByte))
    oVertex2.setProperty("testbool", new java.lang.Boolean(false))

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("vertextype") -> TestUtils.testSchemaForVertices),
      List(oVertex1, oVertex2))

    val source = new DefaultSource(mockOrientDBGraph.vertexWrapper, null, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForVertices)

    val filters: Array[Filter] = Array(
      EqualTo("testbool", true),
      EqualTo("teststring", "Hello")
    )

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), filters)

    assert(rdd.collect().contains(Row(1, true)))
  }

  test("DefaultSource supports user schema, pruned and filtered scans for Edges") {
    val params = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "edgetype" -> "test_edge")

    val oEdge2 = new MockEdge(Mockito.mock(classOf[OrientBaseGraph]), new ODocument())
    oEdge2.setProperty("src", new Integer(1))
    oEdge2.setProperty("dst", new Integer(2))
    oEdge2.setProperty("relationship", new String("friend"))

    val mockOrientDBGraph = new MockOrientDBGraph(Map(params("edgetype") -> TestUtils.testSchemaForEdges),
      null, List(oEdge2))

    val source = new DefaultSource(null, mockOrientDBGraph.edgeWrapper, _ => mockOrientDBClient)
    val relation = source.createRelation(sqlContext, params, TestUtils.testSchemaForEdges)

    val filters: Array[Filter] = Array(
      EqualTo("relationship", "friend")
    )

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("relationship"), filters)

    val prunedExpectedValues = Array(
      Row("friend")
    )
    assert(rdd.collect() === prunedExpectedValues)
  }

  test("Cannot save when 'query' parameter is specified instead of 'vertextype' for Vertices") {
    val invalidParams = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "query" -> "select testbyte, testbool from test_vertex where teststring = '\\Unicode''s樂趣'")

    intercept[IllegalArgumentException] {
      expectedDataDfVertices.write.format("org.apache.spark.orientdb.graphs")
        .options(invalidParams).save()
    }
  }

  test("Cannot save when 'query' parameter is specified instead of 'edgetype' for Edges") {
    val invalidParams = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "query" -> "select relationship from test_edge where relationship = 'enemy'")

    intercept[IllegalArgumentException] {
      expectedDataDfEdges.write.format("org.apache.spark.orientdb.graphs")
        .options(invalidParams).save()
    }
  }

  test("DefaultSource has default constructor, required by Data Source API") {
    new DefaultSource()
  }
}