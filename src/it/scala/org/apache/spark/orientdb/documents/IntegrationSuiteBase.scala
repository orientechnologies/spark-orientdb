package org.apache.spark.orientdb.documents

import com.orientechnologies.orient.core.db.document.ODatabaseDocument
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
  protected var connection: ODatabaseDocument = _

  protected val orientDBWrapper = DefaultOrientDBDocumentWrapper

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext("local", "OrientDBSourceSuite")

    val parameters = Map("dburl" -> ORIENTDB_CONNECTION_URL,
      "user" -> ORIENTDB_USER,
      "password" -> ORIENTDB_PASSWORD,
      "class" -> "dummy")
    connection = orientDBWrapper.getConnection(Parameters.mergeParameters(parameters))
  }

  override def afterAll(): Unit = {
    try {
      connection.close()
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

  def testRoundtripSaveAndLoad(className: String,
                               df: DataFrame,
                               expectedSchemaAfterLoad: Option[StructType] = None,
                               saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    try {
      df.write
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", className)
        .mode(saveMode)
        .save()

      if (!orientDBWrapper.doesClassExists(className)) {
        Thread.sleep(1000)
        assert(orientDBWrapper.doesClassExists(className))
      }

      val loadedDf = sqlContext.read.format("org.apache.spark.orientdb.documents")
                      .option("dburl", ORIENTDB_CONNECTION_URL)
                      .option("user", ORIENTDB_USER)
                      .option("password", ORIENTDB_PASSWORD)
                      .option("class", className)
                      .load()
      assert(loadedDf.schema === expectedSchemaAfterLoad.getOrElse(df.schema))
      checkAnswer(loadedDf, df.collect())
    } finally {
      orientDBWrapper.delete(null, className, null)
      val schema = connection.getMetadata.getSchema
      schema.dropClass(className)
    }
  }
}