package org.apache.spark.orientdb.documents

import org.apache.spark.orientdb.TestUtils
import org.apache.spark.sql.{Row, SaveMode}

class OrientDBIntegrationSuite extends IntegrationSuiteBase {
  private val test_table: String = s"test_table__"
  private val test_table2: String = s"test_table2__"
  private val test_table3: String = s"test_table3__"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema).write
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER).option("password", ORIENTDB_PASSWORD)
      .option("class", test_table)
      .mode(SaveMode.Overwrite)
      .save()
  }

  test("DefaultSource can load OrientDB data to a DataFrame") {
    checkAnswer(
      sqlContext.sql("select * from test_table__"),
      TestUtils.expectedData
    )
  }

  test("count() on DataFrame created from a OrientDB class") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER).option("password", ORIENTDB_PASSWORD)
      .option("class", test_table)
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(5))
    )
  }
}