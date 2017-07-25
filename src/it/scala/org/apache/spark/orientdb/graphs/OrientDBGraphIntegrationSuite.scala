package org.apache.spark.orientdb.graphs

import org.apache.spark.orientdb.TestUtils
import org.apache.spark.orientdb.TestUtils._
import org.apache.spark.orientdb.udts.{EmbeddedList, EmbeddedListType, LinkList, LinkListType}
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

    sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEmbeddedUDTsVertices),
      TestUtils.testSchemaForEmbeddedUDTsForVertices).write
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type)
      .mode(SaveMode.Overwrite)
      .save()

    sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEmbeddedUDTsEdges),
      TestUtils.testSchemaForEmbeddedUDTsForEdges).write
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type)
      .option("edgetype", test_edge_type)
      .mode(SaveMode.Overwrite)
      .save()

    sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForLinkUDTsForVertices),
      TestUtils.testSchemaForLinkUDTsForVertices).write
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type3)
      .mode(SaveMode.Overwrite)
      .save()

    sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForLinkUDTsForEdges),
      TestUtils.testSchemaForLinkUDTsForEdges).write
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type3)
      .option("edgetype", test_edge_type3)
      .mode(SaveMode.Overwrite)
      .save()
  }

  override def afterEach(): Unit = {
    orientDBGraphVertexWrapper.delete(test_vertex_type2, null)
    orientDBGraphVertexWrapper.delete(test_vertex_type, null)
    orientDBGraphVertexWrapper.delete(test_vertex_type3, null)
    orientDBGraphEdgeWrapper.delete(test_edge_type2, null)
    orientDBGraphEdgeWrapper.delete(test_edge_type, null)
    orientDBGraphEdgeWrapper.delete(test_edge_type3, null)
    vertex_connection.dropVertexType(test_vertex_type2)
    vertex_connection.dropVertexType(test_vertex_type)
    vertex_connection.dropVertexType(test_vertex_type3)
    edge_connection.dropEdgeType(test_edge_type2)
    edge_connection.dropEdgeType(test_edge_type)
    edge_connection.dropEdgeType(test_edge_type3)
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

  test("query with pruned and filtered scans for Vertices") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type2)
      .option("query", s"select testbyte, testbool from $test_vertex_type2 " +
        s"where testbool = true and testdouble = 1234152.12312498 " +
        s"and testfloat = 1.0 and testint = 42")
      .schema(StructType(Array(StructField("testbyte", ByteType, true),
        StructField("testbool", BooleanType, true))))
      .load()

    checkAnswer(
      loadedDf,
      Seq(Row(1, true))
    )
  }

  test("query with pruned and filtered scans for Edges") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("edgetype", test_edge_type2)
      .option("query", s"select * from $test_edge_type2 " +
        s"where relationship = 'friends'")
      .load()

    checkAnswer(
      loadedDf,
      Seq(Row(2, "friends", 1), Row(4, "friends", 3))
    )
  }

  test("roundtrip save and load for vertices") {
    val tableName = s"roundtrip_save_and_load_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForVertices),
        TestUtils.testSchemaForVertices).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBGraphVertexWrapper.doesVertexTypeExists(tableName))

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", tableName)
        .load()

      checkAnswer(
        loadedDf.selectExpr("count(*)"),
        Seq(Row(5))
      )
    } finally {
      orientDBGraphVertexWrapper.delete(tableName, null)
      vertex_connection.dropVertexType(tableName)
    }
  }

  test("roundtrip save and load for edges") {
    val vertexTableName = s"roundtrip_save_and_load_vertex_${scala.util.Random.nextInt(100)}"
    val edgeTableName = s"roundtrip_save_and_load_edge_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForVertices),
        TestUtils.testSchemaForVertices).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexTableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBGraphVertexWrapper.doesVertexTypeExists(vertexTableName))

      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEdges),
        TestUtils.testSchemaForEdges).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexTableName)
        .option("edgetype", edgeTableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBGraphEdgeWrapper.doesEdgeTypeExists(edgeTableName))

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("edgetype", edgeTableName)
        .load()

      checkAnswer(
        loadedDf.selectExpr("count(*)"),
        Seq(Row(4))
      )
    } finally {
      try {
        orientDBGraphVertexWrapper.delete(vertexTableName, null)
        vertex_connection.dropVertexType(vertexTableName)
      } finally {
        try {
          orientDBGraphEdgeWrapper.doesEdgeTypeExists(edgeTableName)
          orientDBGraphEdgeWrapper.delete(edgeTableName, null)
        } finally {
          edge_connection.dropEdgeType(edgeTableName)
        }
      }
    }
  }

  test("roundtrip save and load with uppercase column names for vertices") {
    val vertexTableName = s"roundtrip_save_and_load_vertex_${scala.util.Random.nextInt(100)}"

    testRoundtripSaveAndLoadForVertices(vertexTableName,
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1, 1))), StructType(
        Seq(StructField("id", IntegerType, true), StructField("A", IntegerType)))),
      expectedSchemaAfterLoad = Some(StructType(Seq(StructField("id", IntegerType),
        StructField("A", IntegerType)))))
  }

  test("roundtrip save and load with uppercase column names for edges") {
    val vertexTableName = s"roundtrip_save_and_load_vertex_${scala.util.Random.nextInt(100)}"
    val edgeTableName = s"roundtrip_save_and_load_edges_${scala.util.Random.nextInt(100)}"

    testRoundtripSaveAndLoadForEdges(vertexTableName, edgeTableName,
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1), Row(2))),
        StructType(Seq(StructField("id", IntegerType)))),
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1, 2, "friend"))),
        StructType(Seq(StructField("src", IntegerType, true),
          StructField("dst", IntegerType, true),
          StructField("RELATIONSHIP", StringType, true)))))
  }

  test("SaveMode.Overwrite with OrientDB Graph Vertex type name") {
    val vertexType = s"overwrite_schema_qualified_vertextype_name_${scala.util.Random.nextInt(100)}"

    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
      StructType(Seq(StructField("id", IntegerType))))

    try {
      df.write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(SaveMode.Overwrite)
        .save()

      assert(DefaultOrientDBGraphVertexWrapper.doesVertexTypeExists(vertexType))

      df.write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(SaveMode.Overwrite)
        .save()
    } finally {
      try {
        orientDBGraphVertexWrapper.delete(vertexType, null)
      } finally {
        vertex_connection.dropVertexType(vertexType)
      }
    }
  }

  test("SaveMode.Overwrite with OrientDB Graph Edge type name") {
    val vertexType = s"overwrite_schema_qualified_vertextype_name_${scala.util.Random.nextInt(100)}"
    val edgeType = s"overwrite_schema_qualified_edgetype_name_${scala.util.Random.nextInt(100)}"

    try {
      val vertexDf = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1), Row(2))),
        StructType(Seq(StructField("id", IntegerType))))

      val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1, 2, "friend"))),
        StructType(Seq(StructField("src", IntegerType), StructField("dst", IntegerType),
          StructField("relationship", StringType))))

      vertexDf.write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(SaveMode.ErrorIfExists)
        .save()

      df.write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .option("edgetype", edgeType)
        .mode(SaveMode.Overwrite)
        .save()

      assert(DefaultOrientDBGraphEdgeWrapper.doesEdgeTypeExists(edgeType))

      df.write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .option("edgetype", edgeType)
        .mode(SaveMode.Overwrite)
        .save()
    } finally {
      try {
        orientDBGraphEdgeWrapper.delete(edgeType, null)
        orientDBGraphVertexWrapper.delete(vertexType, null)
      } finally {
        edge_connection.dropEdgeType(edgeType)
        vertex_connection.dropVertexType(vertexType)
      }
    }
  }

  test("SaveMode.Overwrite with non-existent OrientDB Graph vertex type") {
    testRoundtripSaveAndLoadForVertices(
      s"overwrite_non_existent_vertextype_${scala.util.Random.nextInt(100)}",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(Seq(StructField("id", IntegerType)))),
      saveMode = SaveMode.Overwrite)
  }

  test("SaveMode.Overwrite with non-existent OrientDB Graph edge type") {
    testRoundtripSaveAndLoadForEdges(
      s"overwrite_non_existent_vertextype_${scala.util.Random.nextInt(100)}",
      s"overwrite_non_existent_edgetype_${scala.util.Random.nextInt(100)}",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1), Row(2))),
        StructType(Seq(StructField("id", IntegerType)))),
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1, 2, "friend"))),
        StructType(Seq(StructField("src", IntegerType), StructField("dst", IntegerType),
          StructField("relationship", StringType)))),
      saveMode = SaveMode.Overwrite
    )
  }

  test("SaveMode.Overwrite with existing OrientDB Graph vertex type") {
    val vertexType = s"overwrite_existing_vertextype_${scala.util.Random.nextInt(100)}"

    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
      StructType(Seq(StructField("id", IntegerType))))

    try {
      df.write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBGraphVertexWrapper.doesVertexTypeExists(vertexType))

      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForVertices),
        TestUtils.testSchemaForVertices).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(SaveMode.Overwrite)
        .save()

      assert(DefaultOrientDBGraphVertexWrapper.doesVertexTypeExists(vertexType))

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .load()

      checkAnswer(
        loadedDf.selectExpr("count(*)"),
        Seq(Row(5)))
    } finally {
      try {
        orientDBGraphVertexWrapper.delete(vertexType, null)
      } finally {
        vertex_connection.dropVertexType(vertexType)
      }
    }
  }

  test("SaveMode.Overwrite with existing OrientDB Graph edge type") {
    val vertexType = s"overwrite_existing_vertextype_${scala.util.Random.nextInt(100)}"
    val edgeType = s"overwrite_existing_edgetype_${scala.util.Random.nextInt(100)}"

    try {
      val vertexDf = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1), Row(2), Row(3),
        Row(4))), StructType(Seq(StructField("id", IntegerType))))

      val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1, 2, "friend"))),
        StructType(Seq(StructField("src", IntegerType), StructField("dst", IntegerType),
          StructField("relationship", StringType))))

      vertexDf.write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(SaveMode.ErrorIfExists)
        .save()

      df.write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .option("edgetype", edgeType)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBGraphEdgeWrapper.doesEdgeTypeExists(edgeType))

      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEdges),
        TestUtils.testSchemaForEdges).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .option("edgetype", edgeType)
        .mode(SaveMode.Overwrite)
        .save()

      assert(DefaultOrientDBGraphEdgeWrapper.doesEdgeTypeExists(edgeType))

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("edgetype", edgeType)
        .load()

      checkAnswer(
        loadedDf.selectExpr("count(*)"),
        Seq(Row(4))
      )
    } finally {
      try {
        orientDBGraphEdgeWrapper.delete(edgeType, null)
        orientDBGraphVertexWrapper.delete(vertexType, null)
      } finally {
        edge_connection.dropEdgeType(edgeType)
        vertex_connection.dropVertexType(vertexType)
      }
    }
  }

  test("Append SaveMode doesn't destroy existing data for vertices") {
    val vertexType = s"append_vertextype_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForVertices),
        TestUtils.testSchemaForVertices).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBGraphVertexWrapper.doesVertexTypeExists(vertexType))

      val extraData = Seq(
        Row(6, 2.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L,
          24.toShort, "___|_123", null))

      sqlContext.createDataFrame(sc.parallelize(extraData),
        TestUtils.testSchemaForVertices).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(SaveMode.Append)
        .save()

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .load()

      checkAnswer(
        loadedDf.selectExpr("count(*)"),
        Seq(Row((TestUtils.expectedDataForVertices ++ extraData).length))
      )
    } finally {
      try {
        orientDBGraphVertexWrapper.delete(vertexType, null)
      } finally {
        vertex_connection.dropVertexType(vertexType)
      }
    }
  }

  test("Append SaveMode doesn't destroy existing data for edges") {
    val vertexType = s"append_vertextype_${scala.util.Random.nextInt(100)}"
    val edgeType = s"append_edgetype_${scala.util.Random.nextInt(100)}"

    try {
      val vertexDf = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1), Row(2), Row(3),
        Row(4))), StructType(Seq(StructField("id", IntegerType))))

      vertexDf.write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(SaveMode.ErrorIfExists)
        .save()

      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEdges),
        TestUtils.testSchemaForEdges).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .option("edgetype", edgeType)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBGraphEdgeWrapper.doesEdgeTypeExists(edgeType))

      val externalData = Seq(Row(4, 2, "enemy"))

      sqlContext.createDataFrame(sc.parallelize(externalData),
        TestUtils.testSchemaForEdges).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .option("edgetype", edgeType)
        .mode(SaveMode.Append)
        .save()

      assert(DefaultOrientDBGraphEdgeWrapper.doesEdgeTypeExists(edgeType))

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("edgetype", edgeType)
        .load()

      checkAnswer(
        loadedDf.selectExpr("count(*)"),
        Seq(Row((TestUtils.expectedDataForEdges ++ externalData).length))
      )
    } finally {
      try {
        orientDBGraphEdgeWrapper.delete(edgeType, null)
        orientDBGraphVertexWrapper.delete(vertexType, null)
      } finally {
        edge_connection.dropEdgeType(edgeType)
        vertex_connection.dropVertexType(vertexType)
      }
    }
  }

  test("Respect SaveMode.ErrorIfExists when vertextype exists") {
    val vertexType = s"error_vertextype_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForVertices),
        TestUtils.testSchemaForVertices).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(SaveMode.ErrorIfExists)
        .save()

      intercept[RuntimeException] {
        sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForVertices),
          TestUtils.testSchemaForVertices).write
          .format("org.apache.spark.orientdb.graphs")
          .option("dburl", ORIENTDB_CONNECTION_URL)
          .option("user", ORIENTDB_USER)
          .option("password", ORIENTDB_PASSWORD)
          .option("vertextype", vertexType)
          .mode(SaveMode.ErrorIfExists)
          .save()
      }
    } finally {
      orientDBGraphVertexWrapper.delete(vertexType, null)
      vertex_connection.dropVertexType(vertexType)
    }
  }

  test("Respect SaveMode.ErrorIfExists when edgetype exists") {
    val vertexType = s"error_vertextype_${scala.util.Random.nextInt(100)}"
    val edgeType = s"error_edgetype_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForVertices),
        TestUtils.testSchemaForVertices).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(SaveMode.ErrorIfExists)
        .save()

      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEdges),
        TestUtils.testSchemaForEdges).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .option("edgetype", edgeType)
        .mode(SaveMode.ErrorIfExists)
        .save()

      intercept[RuntimeException] {
        sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEdges),
          TestUtils.testSchemaForEdges).write
          .format("org.apache.spark.orientdb.graphs")
          .option("dburl", ORIENTDB_CONNECTION_URL)
          .option("user", ORIENTDB_USER)
          .option("password", ORIENTDB_PASSWORD)
          .option("vertextype", vertexType)
          .option("edgetype", edgeType)
          .mode(SaveMode.ErrorIfExists)
          .save()
      }
    } finally {
      try {
        orientDBGraphEdgeWrapper.delete(edgeType, null)
        orientDBGraphVertexWrapper.delete(vertexType, null)
      } finally {
        edge_connection.dropEdgeType(edgeType)
        vertex_connection.dropVertexType(vertexType)
      }
    }
  }

  test("Do nothing when OrientDB Graph vertextype exists if SaveMode = Ignore") {
    val vertexType = s"ignore_vertexType_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForVertices),
        TestUtils.testSchemaForVertices).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBGraphVertexWrapper.doesVertexTypeExists(vertexType))

      val extraData = Seq(
        Row(6, 2.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L,
          24.toShort, "___|_123", null))

      sqlContext.createDataFrame(sc.parallelize(extraData),
        TestUtils.testSchemaForVertices).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(SaveMode.Ignore)
        .save()

      val loadedDf = sqlContext.read
                        .format("org.apache.spark.orientdb.graphs")
                        .option("dburl", ORIENTDB_CONNECTION_URL)
                        .option("user", ORIENTDB_USER)
                        .option("password", ORIENTDB_PASSWORD)
                        .option("vertextype", vertexType)
                        .load()

      checkAnswer(
        loadedDf.selectExpr("count(*)"),
        Seq(Row(TestUtils.expectedDataForVertices.length))
      )
    } finally {
      try {
        orientDBGraphVertexWrapper.delete(vertexType, null)
      } finally {
        vertex_connection.dropVertexType(vertexType)
      }
    }
  }

  test("Do nothing when OrientDB Graph edgetype exists if SaveMode = Ignore") {
    val vertexType = s"ignore_vertextype_${scala.util.Random.nextInt(100)}"
    val edgeType = s"ignore_edgetype_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForVertices),
        TestUtils.testSchemaForVertices).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBGraphVertexWrapper.doesVertexTypeExists(vertexType))

      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEdges),
        TestUtils.testSchemaForEdges).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .option("edgetype", edgeType)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBGraphEdgeWrapper.doesEdgeTypeExists(edgeType))

      val extraData = Seq(Row(4, 2, "enemy"))

      sqlContext.createDataFrame(sc.parallelize(extraData),
        TestUtils.testSchemaForEdges).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .option("edgetype", edgeType)
        .mode(SaveMode.Ignore)
        .save()

      val loadedDf = sqlContext.read
                      .format("org.apache.spark.orientdb.graphs")
                      .option("dburl", ORIENTDB_CONNECTION_URL)
                      .option("user", ORIENTDB_USER)
                      .option("password", ORIENTDB_PASSWORD)
                      .option("edgetype", edgeType)
                      .load()

      checkAnswer(
        loadedDf.selectExpr("count(*)"),
        Seq(Row(4))
      )
    } finally {
      try {
        orientDBGraphEdgeWrapper.delete(edgeType, null)
        orientDBGraphVertexWrapper.delete(vertexType, null)
      } finally {
        edge_connection.dropEdgeType(edgeType)
        vertex_connection.dropVertexType(vertexType)
      }
    }
  }

  test("count() on DataFrame created from a OrientDB Graph vertex type with Embedded types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type)
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(5))
    )
  }

  test("count() on DataFrame created from a OrientDB Graph edge type with Embedded types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("edgetype", test_edge_type)
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(4))
    )
  }

  test("count() on DataFrame created from a OrientDB Graph vertex type with Link Types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type3)
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(5))
    )
  }

  test("count() on DataFrame created from a OrientDB Graph edge type with Link Types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("edgetype", test_edge_type3)
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(4))
    )
  }

  test("count() on DataFrame created from a OrientDB Graph vertex query with Embedded types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type)
      .option("query", s"select * from $test_vertex_type limit 1")
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(1))
    )
  }

  test("count() on DataFrame created from a OrientDB Graph edge query with Embedded types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("edgetype", test_edge_type)
      .option("query", s"select * from $test_edge_type limit 1")
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(1))
    )
  }

  test("count() on DataFrame created from a OrientDB Graph vertex query with Link types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type)
      .option("query", s"select * from $test_vertex_type3 limit 1")
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(1))
    )
  }

  test("count() on DataFrame created from a OrientDB Graph edge query with Link Types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("edgetype", test_edge_type)
      .option("query", s"select * from $test_edge_type3 limit 1")
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(1))
    )
  }

  test("Can load output when 'query' is specified with user-defined schema " +
    "for OrientDB Vertices for Embedded types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type)
      .option("query", s"select embeddedlist from $test_vertex_type")
      .schema(StructType(Array(
        StructField("embeddedlist", EmbeddedListType, true))))
      .load()

    checkAnswer(
      loadedDf,
      Seq(
        Row(EmbeddedList(Array(1, 1.toByte, true, TestUtils.toDate(2015, 6, 1), 1234152.12312498,
          1.0f, 42, 1239012341823719L, 23.toShort, "Unicode's樂趣",
          TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1)))),
        Row(EmbeddedList(Array(2, 1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
          1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)))),
        Row(EmbeddedList(Array(3, 0.toByte, null, TestUtils.toDate(2015, 6, 3), 0.0, -1.0f, 4141214,
          1239012341823719L, null, "f", TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0)))),
        Row(EmbeddedList(Array(4, 0.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L, 24.toShort,
          "___|_123", null))),
        Row(EmbeddedList(Array.fill(11)(null))))
    )
  }

  test("Can load output when 'query' is specified with user-defined schema " +
    "for OrientDB Edges for Embedded types") {
    try {
      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("edgetype", test_edge_type)
        .option("query", s"select embeddedlist from $test_edge_type")
        .schema(StructType(Array(
          StructField("embeddedlist", EmbeddedListType, true))))
        .load()

      checkAnswer(
        loadedDf,
        Seq(
          Row(EmbeddedList(Array(1, 2, "friends"))),
          Row(EmbeddedList(Array(2, 3, "enemy"))),
          Row(EmbeddedList(Array(3, 4, "friends"))),
          Row(EmbeddedList(Array(4, 1, "enemy")))
        )
      )
    } catch {
      case e: Exception => LOG.info("Bug in Orient Graph Edges Read Api")
    }
  }

  test("Can load output when 'query' is specified with user-defined schema " +
    "for OrientDB Vertices for Link types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type3)
      .option("query", s"select linklist from $test_vertex_type3")
      .schema(StructType(Array(
        StructField("linklist", LinkListType, true))))
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(5)))
  }

  test("Can load output when 'query' is specified with user-defined schema " +
    "for OrientDB Edges for Link types") {
    try {
      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("edgetype", test_edge_type3)
        .option("query", s"select linklist from $test_edge_type3")
        .schema(StructType(Array(
          StructField("linklist", LinkListType, true))))
        .load()

      checkAnswer(
        loadedDf.selectExpr("count(*)"),
        Seq(Row(4))
      )
    } catch {
      case e: Exception => LOG.info("Bug in Orient Graph Edges Read Api")
    }
  }

  test("query with pruned and filtered scans for embedded types for OrientDB Vertices") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type)
      .option("query", s"select embeddedlist " +
        s"from $test_vertex_type where 'asdf' in embeddedlist")
      .schema(StructType(Array(StructField("embeddedlist", EmbeddedListType, true))))
      .load()

    checkAnswer(loadedDf,
      Seq(Row(EmbeddedList(Array(2, 1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
        1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0))))))
  }

  test("roundtrip save and load for Embedded Types for Vertices") {
    val tableName = s"roundtrip_save_and_load_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEmbeddedUDTsVertices),
        TestUtils.testSchemaForEmbeddedUDTsForVertices).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", tableName)
        .mode(SaveMode.Overwrite)
        .save()

      assert(DefaultOrientDBGraphVertexWrapper.doesVertexTypeExists(tableName))

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", tableName)
        .load()

      checkAnswer(
        loadedDf.selectExpr("count(*)"),
        Seq(Row(5))
      )
    } finally {
      orientDBGraphVertexWrapper.delete(tableName, null)
      vertex_connection.dropVertexType(tableName)
    }
  }

  test("roundtrip save and load for Embedded Types for Edges") {
    val vertexTableName = s"roundtrip_save_and_load_vertex_${scala.util.Random.nextInt(100)}"
    val edgeTableName = s"roundtrip_save_and_load_edge_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEmbeddedUDTsVertices),
        TestUtils.testSchemaForEmbeddedUDTsForVertices).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexTableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBGraphVertexWrapper.doesVertexTypeExists(vertexTableName))

      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEmbeddedUDTsEdges),
        TestUtils.testSchemaForEmbeddedUDTsForEdges).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexTableName)
        .option("edgetype", edgeTableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBGraphEdgeWrapper.doesEdgeTypeExists(edgeTableName))

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("edgetype", edgeTableName)
        .load()

      checkAnswer(
        loadedDf.selectExpr("count(*)"),
        Seq(Row(4))
      )
    } finally {
      try {
        orientDBGraphVertexWrapper.delete(vertexTableName, null)
        vertex_connection.dropVertexType(vertexTableName)
      } finally {
        try {
          orientDBGraphEdgeWrapper.doesEdgeTypeExists(edgeTableName)
          orientDBGraphEdgeWrapper.delete(edgeTableName, null)
        } finally {
          edge_connection.dropEdgeType(edgeTableName)
        }
      }
    }
  }

  test("roundtrip save and load for Link Types for Vertices") {
    val tableName = s"roundtrip_save_and_load_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForLinkUDTsForVertices),
        TestUtils.testSchemaForLinkUDTsForVertices).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", tableName)
        .mode(SaveMode.Overwrite)
        .save()

      assert(DefaultOrientDBGraphVertexWrapper.doesVertexTypeExists(tableName))

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", tableName)
        .load()

      checkAnswer(
        loadedDf.selectExpr("count(*)"),
        Seq(Row(5))
      )
    } finally {
      orientDBGraphVertexWrapper.delete(tableName, null)
      vertex_connection.dropVertexType(tableName)
    }
  }

  test("roundtrip save and load for Link Types for Edges") {
    val vertexTableName = s"roundtrip_save_and_load_vertex_${scala.util.Random.nextInt(100)}"
    val edgeTableName = s"roundtrip_save_and_load_edge_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForLinkUDTsForVertices),
        TestUtils.testSchemaForLinkUDTsForVertices).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexTableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBGraphVertexWrapper.doesVertexTypeExists(vertexTableName))

      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForLinkUDTsForEdges),
        TestUtils.testSchemaForLinkUDTsForEdges).write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexTableName)
        .option("edgetype", edgeTableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBGraphEdgeWrapper.doesEdgeTypeExists(edgeTableName))

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("edgetype", edgeTableName)
        .load()

      checkAnswer(
        loadedDf.selectExpr("count(*)"),
        Seq(Row(4))
      )
    } finally {
      try {
        orientDBGraphVertexWrapper.delete(vertexTableName, null)
        vertex_connection.dropVertexType(vertexTableName)
      } finally {
        try {
          orientDBGraphEdgeWrapper.doesEdgeTypeExists(edgeTableName)
          orientDBGraphEdgeWrapper.delete(edgeTableName, null)
        } finally {
          edge_connection.dropEdgeType(edgeTableName)
        }
      }
    }
  }
}