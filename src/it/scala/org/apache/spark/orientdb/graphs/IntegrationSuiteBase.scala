package org.apache.spark.orientdb.graphs

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.SparkContext
import org.apache.spark.orientdb.QueryTest
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

trait IntegrationSuiteBase
  extends QueryTest
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  protected def loadConfigFromEnv(envVarName: String): String = {
    Option(System.getenv(envVarName)).getOrElse{
      fail(s"Must set $envVarName environment variable")
    }
  }

  protected val ORIENTDB_CONNECTION_URL = loadConfigFromEnv("ORIENTDB_CONN_URL")
  protected val ORIENTDB_USER = loadConfigFromEnv("ORIENTDB_USER")
  protected val ORIENTDB_PASSWORD = loadConfigFromEnv("ORIENTDB_PASSWORD")

  protected var sc: SparkContext = _
  protected var sqlContext: SQLContext = _
  protected var vertex_connection: OrientGraphNoTx = _
  protected var edge_connection: OrientGraphNoTx = _

  protected val orientDBGraphVertexWrapper = DefaultOrientDBGraphVertexWrapper
  protected val orientDBGraphEdgeWrapper = DefaultOrientDBGraphEdgeWrapper

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext("local", "OrientDBSourceSuite")

    val parameters = scala.collection.mutable.Map[String, String](
                      "dburl" -> ORIENTDB_CONNECTION_URL,
                      "user" -> ORIENTDB_USER,
                      "password" -> ORIENTDB_PASSWORD)

    val vertexParams = parameters.put("vertextype", "dummy_vertex").toMap[String, String]
    val edgeParams = parameters.put("edgetype", "dummy_edge").toMap[String, String]

    vertex_connection = orientDBGraphVertexWrapper
      .getConnection(Parameters.mergeParameters(vertexParams))
    edge_connection = orientDBGraphEdgeWrapper
      .getConnection(Parameters.mergeParameters(edgeParams))
  }

  override def afterAll(): Unit = {
    try {
      orientDBGraphVertexWrapper.close()
      orientDBGraphEdgeWrapper.close()
    } finally {
      try {
        sc.stop()
      } finally {
        super.afterAll()
      }
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sqlContext = new SQLContext(sc)
  }

  def testRoundtripSaveAndLoadForVertices(vertexType: String,
                                          df: DataFrame,
                                          expectedSchemaAfterLoad: Option[StructType] = None,
                                          saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    try {
      df.write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("vertextype", vertexType)
        .mode(saveMode)
        .save()

      if (!orientDBGraphVertexWrapper.doesVertexTypeExists(vertexType)) {
        Thread.sleep(1000)
        assert(orientDBGraphVertexWrapper.doesVertexTypeExists(vertexType))
      }

      val loadedDf = sqlContext.read
                      .format("org.apache.spark.orientdb.graphs")
                      .option("dburl", ORIENTDB_CONNECTION_URL)
                      .option("user", ORIENTDB_USER)
                      .option("password", ORIENTDB_PASSWORD)
                      .option("vertextype", vertexType)
                      .load()

      assert(loadedDf.schema === expectedSchemaAfterLoad.getOrElse(df.schema))
      checkAnswer(loadedDf, df.collect())
    } finally {
      orientDBGraphVertexWrapper.delete(vertexType, null)
      vertex_connection.dropVertexType(vertexType)
    }
  }

  def testRoundtripSaveAndLoadForEdges(edgeType: String,
                                       df: DataFrame,
                                       expectedSchemaAfterLoad: Option[StructType] = None,
                                       saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    try {
      df.write
        .format("org.apache.spark.orientdb.graphs")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("edgetype", edgeType)
        .mode(saveMode)
        .save()

      if (!orientDBGraphEdgeWrapper.doesEdgeTypeExists(edgeType)) {
        Thread.sleep(1000)
        assert(orientDBGraphEdgeWrapper.doesEdgeTypeExists(edgeType))
      }

      val loadedDf = sqlContext.read
                      .format("org.apache.spark.orientdb.graphs")
                      .option("dburl", ORIENTDB_CONNECTION_URL)
                      .option("user", ORIENTDB_USER)
                      .option("password", ORIENTDB_PASSWORD)
                      .option("edgetype", edgeType)
                      .load()

      assert(loadedDf.schema === expectedSchemaAfterLoad.getOrElse(df.schema))
      checkAnswer(loadedDf, df.collect())
    } finally {
      orientDBGraphEdgeWrapper.delete(edgeType, null)
      edge_connection.dropEdgeType(edgeType)
    }
  }
}