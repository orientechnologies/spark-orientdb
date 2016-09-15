package org.apache.spark.orientdb.graphs

import com.tinkerpop.blueprints.{Edge, Vertex}
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx

object TestApplicationForEdge extends App {
  val parameters = Map("dburl" -> "remote:localhost:2424/GratefulDeadConcerts",
                    "user" -> "root",
                    "password" -> "root",
                    "edgeType" -> "e125")

  val graphEdgeWrapper = new OrientDBGraphEdgeWrapper()
  val testEdgeType = "e125"
  val testProperties: Map[String, Object] = Map("id1" -> new Integer(1), "key1" -> "Hello",
                                              "value" -> "World", "test" -> "Test")
  val testRequiredProps = Array("id1", "name")

  assert(getConnectionTest() != null)

  getConnectionTest().createVertexType("v23")
  val testInVertex = getConnectionTest().addVertex("v23", null)
 val testOutVertex = getConnectionTest().addVertex("v23", null)

  assert(!doesEdgeTypeExistTest(testEdgeType))
  getConnectionTest().createEdgeType(testEdgeType)
  assert(
    createTest(testEdgeType, testInVertex, testOutVertex, testProperties))
  readTest(testEdgeType, testRequiredProps, "WHERE id1 = '1'")
  assert(bulkCreateTest(Map(testEdgeType -> (testInVertex, testOutVertex)),
    List(Map("id1" -> new Integer(2), "key1" -> "Hello1",
      "value" -> "World", "test" -> "Test"))))
  assert(deleteTest(testEdgeType, Map("id1" -> (" = ", "2"))))
  resolveTableTest(testEdgeType)
  genericQueryTest(s"select * from $testEdgeType")

  def getConnectionTest(): OrientGraphNoTx = {
    graphEdgeWrapper.getConnection(Parameters.mergeParameters(parameters))
  }

  def doesEdgeTypeExistTest(edgeType: String): Boolean = {
    graphEdgeWrapper.doesEdgeTypeExists(edgeType)
  }

  def createTest(edgeType: String, inVertex: Vertex,
                 outVertex: Vertex, properties: Map[String, Object]): Boolean = {
    graphEdgeWrapper.create(edgeType, inVertex, outVertex, properties)
  }

  def readTest(edgeType: String, requiredProperties: Array[String],
           filters: String): Unit = {
    val edges: List[Edge] = graphEdgeWrapper.read(edgeType, requiredProperties, filters)

    edges.foreach(edge => {
      requiredProperties.foreach(property => {
        println(edge.getProperty(property))
      })
    })
  }

  def bulkCreateTest(edgeTypes: Map[String, Tuple2[Vertex, Vertex]],
                     properties: List[Map[String, Object]]): Boolean = {
    graphEdgeWrapper.bulkCreate(edgeTypes, properties)
  }

  def deleteTest(edgeType: String, filter: Map[String, Tuple2[String, String]]): Boolean = {
    graphEdgeWrapper.delete(edgeType, filter)
  }

  def resolveTableTest(edgeType: String): Unit = {
    val structtype = graphEdgeWrapper.resolveTable(edgeType)

    structtype.fields.foreach(field => {
      println(field.name + " " + field.dataType.typeName + " " + field.nullable)
    })
  }

  def genericQueryTest(query: String): Unit = {
    val edges = graphEdgeWrapper.genericQuery(query)

    println(edges.length)
  }
}