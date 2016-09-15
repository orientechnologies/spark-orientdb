package org.apache.spark.orientdb.graphs

import com.orientechnologies.orient.core.metadata.schema.OType
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.orientdb.graphs.Parameters.MergedParameters

object TestApplication extends App {
  val parameters = Map("dburl" -> "remote:localhost:2424/GratefulDeadConcerts",
                    "user" -> "root",
                    "password" -> "root",
                    "vertexType" -> "a4")

  val graphVertexWrapper = new OrientDBGraphVertexWrapper()
  val testVertexType = "a4"
  val testProperties: Map[String, Object] = Map("id1" -> new Integer(1), "key1" -> "Hello",
    "value" -> "World", "test" -> "Test")
  val testRequiredProps = Array("id1", "key1", "value", "test")

  assert(getConnectionTest() != null)

  if (getConnectionTest().getVertexType(testVertexType) == null) {
    getConnectionTest().createVertexType(testVertexType)

    val gotVertexType = getConnectionTest().getVertexType(testVertexType)
    gotVertexType.createProperty("id1", OType.INTEGER)
    gotVertexType.createProperty("key1", OType.STRING)
    gotVertexType.createProperty("value", OType.STRING)
    gotVertexType.createProperty("test", OType.STRING)
  }
  assert(doesVertexTypeExistTest(testVertexType))
  assert(createTest(testVertexType, testProperties))
  read(testVertexType, testRequiredProps, "WHERE id1 = '1'")
  assert(bulkCreate(List("a2"), List(Map("id1" -> new Integer(2), "key1" -> "Hello",
    "value" -> "World", "test" -> "Test"))))
  assert(delete(testVertexType, Map("id1" -> (" = ", "2"))))
  resolveTable(testVertexType)
  genericQuery(s" select * from $testVertexType")

  def convertParams(): MergedParameters = {
    Parameters.mergeParameters(parameters)
  }

  def getConnectionTest(): OrientGraphNoTx = {
    graphVertexWrapper.getConnection(convertParams())
  }

  def doesVertexTypeExistTest(vertexType: String): Boolean = {
    graphVertexWrapper.doesVertexTypeExists(vertexType)
  }

  def createTest(vertexType: String, properties: Map[String, Object]): Boolean = {
    graphVertexWrapper.create(vertexType, properties)
  }

  def read(vertexType: String, requiredProperties: Array[String],
           filters: String): Unit = {
    val vertices = graphVertexWrapper.read(vertexType, requiredProperties, "")

    vertices.foreach(vertex => {
      requiredProperties.foreach(property => {
        println(vertex.getProperty(property))
      })
    })
  }

  def bulkCreate(vertexTypes: List[String], properties: List[Map[String, Object]]): Boolean = {
    graphVertexWrapper.bulkCreate(vertexTypes, properties)
  }

  def delete(vertexType: String, filter: Map[String, Tuple2[String, String]]): Boolean = {
    graphVertexWrapper.delete(vertexType, filter)
  }

  def resolveTable(vertexType: String): Unit = {
    val structType = graphVertexWrapper.resolveTable(vertexType)
    structType.foreach(field =>
    println(field.name + " " + field.dataType.typeName + " " + field.nullable))
  }

  def genericQuery(query: String): Unit = {
    println(graphVertexWrapper.genericQuery(query).length)
  }


}