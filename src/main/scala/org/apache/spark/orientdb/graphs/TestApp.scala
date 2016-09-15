package org.apache.spark.orientdb.graphs

import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory

object TestApp extends App {
  val graphFactory = new OrientGraphFactory("remote:127.0.0.1:2424/GratefulDeadConcerts",
                                          "root", "root")

  val graph = graphFactory.getNoTx

  val vertexType = graph.createVertexType("v124")
  vertexType.createProperty("id1", OType.INTEGER)
  vertexType.createProperty("name", OType.STRING)

  val inVertex = graph.addVertex("v124", null)
  val outVertex = graph.addVertex("v124", null)

  val edgeType = graph.createEdgeType("e124")
  edgeType.createProperty("id1", OType.INTEGER)
  edgeType.createProperty("name", OType.STRING)

  val edge = graph.addEdge(null, inVertex, outVertex, "e124")
  edge.setProperty("id1", 1)
  edge.setProperty("name", "Subhobrata")

  val query = graph.command(new OCommandSQL("select id1,name from e124"))
    .execute[java.lang.Iterable[Edge]]()

  val iterator = query.iterator()

  while (iterator.hasNext) {
    val data = iterator.next()
    println()
  }
}