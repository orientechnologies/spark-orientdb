package org.apache.spark.orientdb.documents

import org.scalatest.FunSuite

class TableNameSuite extends FunSuite {

  test("escaped table name") {
    val tableName = new TableName("test_table")
    assert(tableName.unescapedTableName === "test_table")
  }

  test("table Name to String") {
    val tableName = new TableName("test_table")
    assert(tableName.toString === "test_table")
  }
}