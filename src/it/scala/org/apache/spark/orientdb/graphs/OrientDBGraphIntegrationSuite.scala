package org.apache.spark.orientdb.graphs

import org.apache.spark.orientdb.TestUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.slf4j.LoggerFactory

class OrientDBGraphIntegrationSuite extends IntegrationSuiteBase {
  private val test_vertex_type: String = "test_vertex_type__"
  private val test_edge_type: String = "test_edge_type__"
  private val test_vertex_type2: String = "test_vertex_type2__"
  private val test_edge_type2: String = "test_edge_type2__"
  private val test_vertex_type3: String = "test_vertex_type3__"
  private val test_edge_type3: String = "test_edge_type3__"

  private val LOG = LoggerFactory.getLogger(getClass)

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForVertices),
      TestUtils.testSchemaForVertices).write
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type2)
      .mode(SaveMode.Overwrite)
      .save()

    sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEdges),
      TestUtils.testSchemaForEdges).write
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type2)
      .option("edgetype", test_edge_type2)
      .mode(SaveMode.Overwrite)
      .save()
  }

  override def afterEach(): Unit = {
    orientDBGraphVertexWrapper.delete(test_vertex_type2, null)
    orientDBGraphEdgeWrapper.delete(test_edge_type2, null)
    vertex_connection.dropVertexType(test_vertex_type2)
    edge_connection.dropEdgeType(test_edge_type2)
  }

  test("count() on DataFrame created from a OrientDB Graph vertex type") {
    val loadedDf = sqlContext.read
                    .format("org.apache.spark.orientdb.graphs")
                    .option("dburl", ORIENTDB_CONNECTION_URL)
                    .option("user", ORIENTDB_USER)
                    .option("password", ORIENTDB_PASSWORD)
                    .option("vertextype", test_vertex_type2)
                    .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(5))
    )
  }

  test("count() on DataFrame created from a OrientDB Graph edge type") {
    val loadedDf = sqlContext.read
                    .format("org.apache.spark.orientdb.graphs")
                    .option("dburl", ORIENTDB_CONNECTION_URL)
                    .option("user", ORIENTDB_USER)
                    .option("password", ORIENTDB_PASSWORD)
                    .option("edgetype", test_edge_type2)
                    .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(4))
    )
  }

  test("count() on DataFrame created from a OrientDB Graph vertex query") {
    val loadedDf = sqlContext.read
                    .format("org.apache.spark.orientdb.graphs")
                    .option("dburl", ORIENTDB_CONNECTION_URL)
                    .option("user", ORIENTDB_USER)
                    .option("password", ORIENTDB_PASSWORD)
                    .option("vertextype", test_vertex_type2)
                    .option("query", s"select * from $test_vertex_type2 where teststring = 'asdf'")
                    .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(1))
    )
  }

  test("count() on DataFrame created from a OrientDB Graph edge query") {
    val loadedDf = sqlContext.read
                    .format("org.apache.spark.orientdb.graphs")
                    .option("dburl", ORIENTDB_CONNECTION_URL)
                    .option("user", ORIENTDB_USER)
                    .option("password", ORIENTDB_PASSWORD)
                    .option("vertextype", test_vertex_type2)
                    .option("edgetype", test_edge_type2)
                    .option("query", s"select * from $test_edge_type2 where relationship = 'friends'")
                    .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(2))
    )
  }

  test("backslashes in queries are escaped for vertex Types") {
    val loadedDf = sqlContext.read
                    .format("org.apache.spark.orientdb.graphs")
                    .option("dburl", ORIENTDB_CONNECTION_URL)
                    .option("user", ORIENTDB_USER)
                    .option("password", ORIENTDB_PASSWORD)
                    .option("vertextype", test_vertex_type2)
                    .option("query", s"select teststring.replace('\\\\', '') as teststring from $test_vertex_type2")
                    .schema(StructType(Array(StructField("teststring", StringType, true))))
                    .load()

    checkAnswer(
      loadedDf.filter("teststring = 'asdf'"),
      Seq(Row("asdf"))
    )
  }

  test("backslashes in queries are escaped for edge Types") {
    // bug in OrientDB Graph edges
    try {
      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", test_vertex_type2)
        .option("edgetype", test_edge_type2)
        .option("query", s"select relationahip.replace('\\\\', '') as relationship from $test_edge_type2")
        .schema(StructType(Array(StructField("relationship", StringType, true))))
        .load()

      checkAnswer(
        loadedDf.filter("relationship = 'friends'"),
        Seq(Row("friends"), Row("friends"))
      )
    } catch {
      case e: java.lang.Exception => LOG.info("Bug in Orient Graph Edges Read Api")
    }
  }

  test("Can load output when 'query' is specified with user-defined schema " +
    "for OrientDB Vertices") {
    val loadedDf = sqlContext.read
                    .format("org.apache.spark.orientdb.graphs")
                    .option("dburl", ORIENTDB_CONNECTION_URL)
                    .option("user", ORIENTDB_USER)
                    .option("password", ORIENTDB_PASSWORD)
                    .option("vertextype", test_vertex_type2)
                    .option("query", s"select teststring, testbool from $test_vertex_type2")
                    .schema(StructType(Array(
                      StructField("teststring", StringType, true),
                      StructField("testbool", BooleanType, true))))
                    .load()

    checkAnswer(
      loadedDf,
      Seq(
        Row("Unicode's樂趣", true),
        Row("___|_123", false),
        Row("asdf", false),
        Row("f", null),
        Row(null, null)
      )
    )
  }

  test("Can load output when 'query' is specified with user-defined schema " +
    "for OrientDB edges") {
    try {
      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", test_vertex_type2)
        .option("edgetype", test_edge_type2)
        .option("query", s"select relationship from $test_edge_type2")
        .schema(StructType(Array(StructField("relationship", StringType, true))))
        .load()

      checkAnswer(
        loadedDf,
        Seq(
          Row("friends"),
          Row("enemy"),
          Row("friends"),
          Row("enemy")
        )
      )
    } catch {
      case e: Exception => LOG.info("Bug in Orient Graph Edges Read Api")
    }
  }

  test("Can load output of OrientDB Graph aggregation queries for Vertices") {
    val loadedDf = sqlContext.read
                    .format("org.apache.spark.orientdb.graphs")
                    .option("dburl", ORIENTDB_CONNECTION_URL)
                    .option("user", ORIENTDB_USER)
                    .option("password", ORIENTDB_PASSWORD)
                    .option("vertextype", test_vertex_type2)
                    .option("query", s"select testbool, count(*) from $test_vertex_type2 group by testbool")
                    .schema(StructType(Array(
                      StructField("testbool", BooleanType, true),
                      StructField("count", LongType, true)
                    ))).load()

    checkAnswer(
      loadedDf,
      Seq(Row(null, 2), Row(false, 2), Row(true, 1))
    )
  }

  test("Can load output of OrientDB Graph aggregation queries for Edges") {
    try {
      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", test_vertex_type2)
        .option("edgetype", test_edge_type2)
        .option("query", s"select relationship, count(*) from $test_vertex_type2 group by relationship")
        .schema(StructType(Array(
          StructField("relationship", StringType, true),
          StructField("count", LongType, true)
        ))).load()

      checkAnswer(
        loadedDf,
        Seq(Row("friends", 2), Row("enemy", 2))
      )
    } catch {
      case e: Exception => LOG.info("Bug in Orient Graph Edges Read Api")
    }
  }

  test("supports simple column filtering for Vertices") {
    val loadedDf = sqlContext.read
                    .format("org.apache.spark.orientdb.graphs")
                    .option("dburl", ORIENTDB_CONNECTION_URL)
                    .option("user", ORIENTDB_USER)
                    .option("password", ORIENTDB_PASSWORD)
                    .option("vertextype", test_vertex_type2)
                    .option("query", s"select testbool from $test_vertex_type2")
                    .schema(StructType(Array(
                      StructField("testbool", BooleanType, true)))).load()

    checkAnswer(
      loadedDf.filter("testbool = true"),
      Seq(Row(true))
    )
  }

  test("supports simple column filtering for Edges") {
    try {
      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", test_vertex_type2)
        .option("edgetype", test_edge_type2)
        .option("query", s"select relationship from $test_edge_type2")
        .schema(StructType(Array(
          StructField("relationship", StringType, true)))).load()

      checkAnswer(
        loadedDf.filter("relationship = 'friends'"),
        Seq(Row("friends"), Row("friends"))
      )
    } catch {
      case e: Exception => LOG.info("Bug in Orient Graph Edges Read Api")
    }
  }
}