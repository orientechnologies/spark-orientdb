package org.apache.spark.orientdb.graphs

import org.apache.spark.orientdb.documents.IntegrationSuiteBase

class OrientDBGraphIntegrationSuite extends IntegrationSuiteBase {
  private val test_vertex_type: String = "test_vertex_type__"
  private val test_edge_type: String = "test_edge_type__"
  private val test_vertex_type2: String = "test_vertex_type2__"
  private val test_edge_type2: String = "test_edge_type2__"
  private val test_vertex_type3: String = "test_vertex_type3__"
  private val test_edge_type3: String = "test_edge_type3__"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override def beforeEach(): Unit = {

  }
}