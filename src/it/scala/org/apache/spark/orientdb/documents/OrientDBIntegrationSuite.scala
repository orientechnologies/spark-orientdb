package org.apache.spark.orientdb.documents

import org.apache.spark.orientdb.TestUtils
import org.apache.spark.orientdb.udts.{EmbeddedList, EmbeddedListType, LinkListType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
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
    sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEmbeddedUDTs), TestUtils.testSchemaForEmbeddedUDTs)
      .write
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER).option("password", ORIENTDB_PASSWORD)
      .option("class", test_table2)
      .mode(SaveMode.Overwrite)
      .save()
    sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForLinkUDTs), TestUtils.testSchemaForLinkUDTs)
      .write
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER).option("password", ORIENTDB_PASSWORD)
      .option("class", test_table3)
      .mode(SaveMode.Overwrite)
      .save()
  }

  override def afterEach(): Unit = {
    orientDBWrapper.delete(null, test_table, null)
    orientDBWrapper.delete(null, test_table2, null)
    orientDBWrapper.delete(null, test_table3, null)
    val schema = connection.getMetadata.getSchema
    schema.dropClass(test_table)
    schema.dropClass(test_table2)
    schema.dropClass(test_table3)
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

  test("count() on DataFrame created from a OrientDB query") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("class", test_table)
      .option("query", s"select * from $test_table where teststring = 'asdf'")
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(1))
    )
  }

  test("backslashes in queries are escaped") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("query", s"select teststring.replace('\\\\', '') as teststring from $test_table")
      .schema(StructType(Array(StructField("teststring", StringType, true))))
      .load()

    checkAnswer(
      loadedDf.filter("teststring = 'asdf'"),
      Seq(Row("asdf")))
  }

  test("Can load output when 'query' is specified with user-defined schema") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("query", s"select teststring, testbool from $test_table")
      .schema(StructType(Array(StructField("teststring", StringType, true),
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

  test("Can load output of OrientDB aggregation queries") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("query", s"select testbool, count(*) from $test_table group by testbool")
      .schema(StructType(Array(StructField("testbool", BooleanType, true),
        StructField("count", LongType, true))))
      .load()

    checkAnswer(
      loadedDf,
      Seq(Row(null, 2), Row(false, 2), Row(true, 1))
    )
  }

  test("supports simple column filtering") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("query", s"select testbool from $test_table")
      .schema(StructType(Array(StructField("testbool", BooleanType, true))))
      .load()

    checkAnswer(
      loadedDf.filter("testbool = true"),
      Seq(Row(true))
    )
  }

  test("query with pruned and filtered scans") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("query", "select testbyte, testbool " +
        s"from $test_table where testbool = true" +
        " and testdouble = 1234152.12312498 and testfloat = 1.0 and testint = 42")
      .schema(StructType(Array(StructField("testbyte", ByteType, true),
        StructField("testbool", BooleanType, true))))
      .load()

    checkAnswer(loadedDf,
      Seq(Row(1, true)))
  }

  test("roundtrip save and load") {
    val tableName = s"roundtrip_save_and_load_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema)
        .write
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBDocumentWrapper.doesClassExists(tableName))

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .load()

      checkAnswer(loadedDf.selectExpr("count(*)"),
        Seq(Row(5))
      )
    } finally {
      orientDBWrapper.delete(null, tableName, null)
      val schema = connection.getMetadata.getSchema
      schema.dropClass(tableName)
    }
  }

  test("roundtrip save and load with uppercase column names") {
    val tableName = s"roundtrip_save_and_load_${scala.util.Random.nextInt(100)}"

    testRoundtripSaveAndLoad(tableName, sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
      StructType(Array(StructField("A", IntegerType, true)))),
      expectedSchemaAfterLoad = Some(StructType(StructField("A", IntegerType) :: Nil)))
  }

  test("SaveMode.Overwrite with table name") {
    val tableName = s"overwrite_schema_qualified_table_name_${scala.util.Random.nextInt(100)}"

    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
      StructType(StructField("a", IntegerType):: Nil))

    try {
        df.write
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .mode(SaveMode.Overwrite)
        .save()

      assert(DefaultOrientDBDocumentWrapper.doesClassExists(tableName))

      df.write
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .mode(SaveMode.Overwrite)
        .save()
    } finally {
      orientDBWrapper.delete(null, tableName, null)
      val schema = connection.getMetadata.getSchema
      schema.dropClass(tableName)
    }
  }

  test("SaveMode.Overwrite with non-existent table") {
    testRoundtripSaveAndLoad(
      s"overwrite_non_existent_table_${scala.util.Random.nextInt(100)}",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("a", IntegerType) :: Nil)),
      saveMode = SaveMode.Overwrite
    )
  }

  test("SaveMode.Overwrite with existing table") {
    val tableName = s"overwrite_exisitng_table_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("a", IntegerType) :: Nil))
        .write
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBDocumentWrapper.doesClassExists(tableName))

      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData),
        TestUtils.testSchema).write
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .mode(SaveMode.Overwrite)
        .save()

      assert(DefaultOrientDBDocumentWrapper.doesClassExists(tableName))
      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .load()

      checkAnswer(loadedDf.selectExpr("count(*)"), Seq(Row(TestUtils.expectedData.length)))
    } finally {
      orientDBWrapper.delete(null, tableName, null)
      val schema = connection.getMetadata.getSchema
      schema.dropClass(tableName)
    }
  }

  test("Append SaveMode doesn't destroy existing data") {
    val tableName = s"append_table_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData),
        TestUtils.testSchema)
        .write
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save

      val extraData = Seq(
        Row(2.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L,
          24.toShort, "___|_123", null))

      sqlContext.createDataFrame(sc.parallelize(extraData),
        TestUtils.testSchema)
        .write
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .mode(SaveMode.Append)
        .save

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .load()

      checkAnswer(loadedDf.selectExpr("count(*)"), Seq(Row((TestUtils.expectedData ++ extraData).length)))
    } finally {
      orientDBWrapper.delete(null, tableName, null)
      val schema = connection.getMetadata.getSchema
      schema.dropClass(tableName)
    }
  }

  test("Respect SaveMode.ErrorIfExists when table exists") {
    val tableName = s"error_table_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData),
        TestUtils.testSchema)
        .write
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      intercept[RuntimeException] {
        sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData),
          TestUtils.testSchema)
          .write
          .format("org.apache.spark.orientdb.documents")
          .option("dburl", ORIENTDB_CONNECTION_URL)
          .option("user", ORIENTDB_USER)
          .option("password", ORIENTDB_PASSWORD)
          .option("class", tableName)
          .mode(SaveMode.ErrorIfExists)
          .save()
      }
    } finally {
      orientDBWrapper.delete(null, tableName, null)
      val schema = connection.getMetadata.getSchema
      schema.dropClass(tableName)
    }
  }

  test("Do nothing when table exists if SaveMode = Ignore") {
    val tableName = s"ignore_table_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData),
        TestUtils.testSchema)
        .write
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      val extraData = Seq(
        Row(2.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L,
          24.toShort, "___|_123", null))

      sqlContext.createDataFrame(sc.parallelize(extraData),
        TestUtils.testSchema)
        .write
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .mode(SaveMode.Ignore)
        .save()

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .load()

      checkAnswer(loadedDf.selectExpr("count(*)"), Seq(Row(TestUtils.expectedData.length)))
    } finally {
      orientDBWrapper.delete(null, tableName, null)
      val schema = connection.getMetadata.getSchema
      schema.dropClass(tableName)
    }
  }

  test("filtering based on date constants") {
    pending
    val date = TestUtils.toDate(year = 2016, zeroBasedMonth = 8, date = 3)

    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("query", s"select testdate from $test_table where testdate = '$date'")
      .schema(StructType(StructField("testdate", DateType, true) :: Nil))
      .load()

    checkAnswer(loadedDf, Seq(Row(date)))
  }

  test("count() on DataFrame created from a OrientDB class with Embedded types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER).option("password", ORIENTDB_PASSWORD)
      .option("class", test_table2)
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(5))
    )
  }

  test("count() on DataFrame created from a OrientDB class with Link types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER).option("password", ORIENTDB_PASSWORD)
      .option("class", test_table3)
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(5))
    )
  }

  test("count() on DataFrame created from a OrientDB query with Embedded types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("class", test_table2)
      .option("query", s"select * from $test_table2 limit 1")
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(1))
    )
  }

  test("count() on DataFrame created from a OrientDB query with Link types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("class", test_table3)
      .option("query", s"select * from $test_table3 limit 1")
      .load()

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(1))
    )
  }

  test("Can load output when 'query' is specified with user-defined schema with Embedded types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("class", test_table2)
      .option("query", s"select embeddedlist from $test_table2")
      .schema(StructType(Array(StructField("embeddedlist", EmbeddedListType, true))))
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

  test("Can load output when 'query' is specified with user-defined schema with Link types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("class", test_table3)
      .option("query", s"select linklist from $test_table3")
      .schema(StructType(Array(StructField("linklist", LinkListType, true))))
      .load()

    loadedDf.schema === StructType(Array(StructField("linklist", LinkListType, true)))

    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(5))
    )
  }

  test("query with pruned and filtered scans for embedded types") {
    val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("query", "select embeddedlist " +
        s"from $test_table2 where 'asdf' in embeddedlist")
      .schema(StructType(Array(StructField("embeddedlist", EmbeddedListType, true))))
      .load()

    checkAnswer(loadedDf,
      Seq(Row(EmbeddedList(Array(2, 1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
        1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0))))))
  }

  test("roundtrip save and load for Embedded Types") {
    val tableName = s"roundtrip_save_and_load_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForEmbeddedUDTs), TestUtils.testSchemaForEmbeddedUDTs)
        .write
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBDocumentWrapper.doesClassExists(tableName))

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .load()

      checkAnswer(loadedDf.selectExpr("count(*)"),
        Seq(Row(5)))
    } finally {
      orientDBWrapper.delete(null, tableName, null)
      val schema = connection.getMetadata.getSchema
      schema.dropClass(tableName)
    }
  }

  test("roundtrip save and load for Link Types") {
    val tableName = s"roundtrip_save_and_load_${scala.util.Random.nextInt(100)}"

    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedDataForLinkUDTs), TestUtils.testSchemaForLinkUDTs)
        .write
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultOrientDBDocumentWrapper.doesClassExists(tableName))

      val loadedDf = sqlContext.read
        .format("org.apache.spark.orientdb.documents")
        .option("dburl", ORIENTDB_CONNECTION_URL)
        .option("user", ORIENTDB_USER)
        .option("password", ORIENTDB_PASSWORD)
        .option("class", tableName)
        .load()

      checkAnswer(loadedDf.selectExpr("count(*)"),
        Seq(Row(5)))
    } finally {
      orientDBWrapper.delete(null, tableName, null)
      val schema = connection.getMetadata.getSchema
      schema.dropClass(tableName)
    }
  }
}