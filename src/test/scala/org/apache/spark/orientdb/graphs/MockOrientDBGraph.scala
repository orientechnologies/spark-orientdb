package org.apache.spark.orientdb.graphs

import com.tinkerpop.blueprints.{Edge, Vertex}
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.orientdb.graphs.Parameters.MergedParameters
import org.apache.spark.sql.types.StructType
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

class MockOrientDBGraph(existingTablesAndSchemas: Map[String, StructType],
                        oVertices: List[Vertex] = null,
                        oEdges: List[Edge] = null) {
  val vertexWrapper: OrientDBGraphVertexWrapper = spy(new OrientDBGraphVertexWrapper())
  val edgeWrapper: OrientDBGraphEdgeWrapper = spy(new OrientDBGraphEdgeWrapper())

  doAnswer(new Answer[OrientGraphNoTx] {
    override def answer(invocationOnMock: InvocationOnMock): OrientGraphNoTx = {
      mock(classOf[OrientGraphNoTx], RETURNS_SMART_NULLS)
    }
  }).when(vertexWrapper).getConnection(any(classOf[MergedParameters]))

  doAnswer(new Answer[OrientGraphNoTx] {
    override def answer(invocationOnMock: InvocationOnMock): OrientGraphNoTx = {
      mock(classOf[OrientGraphNoTx], RETURNS_SMART_NULLS)
    }
  }).when(edgeWrapper).getConnection(any(classOf[MergedParameters]))

  doAnswer(new Answer[Boolean] {
    override def answer(invocationOnMock: InvocationOnMock): Boolean = {
      existingTablesAndSchemas.contains(invocationOnMock.getArguments()(1).asInstanceOf[String])
    }
  }).when(vertexWrapper).doesVertexTypeExists(any(classOf[String]))

  doAnswer(new Answer[Boolean] {
    override def answer(invocationOnMock: InvocationOnMock): Boolean = {
      existingTablesAndSchemas.contains(invocationOnMock.getArguments()(1).asInstanceOf[String])
    }
  }).when(edgeWrapper).doesEdgeTypeExists(any(classOf[String]))

  doAnswer(new Answer[Boolean] {
    override def answer(invocationOnMock: InvocationOnMock): Boolean = {
      true
    }
  }).when(vertexWrapper).create(any(classOf[String]), any(classOf[Map[String, Object]]))

  doAnswer(new Answer[Boolean] {
    override def answer(invocationOnMock: InvocationOnMock): Boolean = {
      true
    }
  }).when(edgeWrapper).create(any(classOf[String]), any(classOf[Vertex]),
    any(classOf[Vertex]), any(classOf[Map[String, Object]]))

  doAnswer(new Answer[List[Vertex]] {
    override def answer(invocationOnMock: InvocationOnMock): List[Vertex] = {
      oVertices
    }
  }).when(vertexWrapper).read(any(classOf[String]), any(classOf[Array[String]]),
    any(classOf[String]), any(classOf[String]))

  doAnswer(new Answer[List[Edge]] {
    override def answer(invocationOnMock: InvocationOnMock): List[Edge] = {
      oEdges
    }
  }).when(edgeWrapper).read(any(classOf[String]), any(classOf[Array[String]]),
    any(classOf[String]), any(classOf[String]))

  doAnswer(new Answer[Boolean] {
    override def answer(invocationOnMock: InvocationOnMock): Boolean = {
      true
    }
  }).when(vertexWrapper).delete(any(classOf[String]),
    any(classOf[Map[String, Tuple2[String, String]]]))

  doAnswer(new Answer[Boolean] {
    override def answer(invocationOnMock: InvocationOnMock): Boolean = {
      true
    }
  }).when(edgeWrapper).delete(any(classOf[String]),
    any(classOf[Map[String, Tuple2[String, String]]]))

  doAnswer(new Answer[StructType] {
    override def answer(invocationOnMock: InvocationOnMock): StructType = {
      existingTablesAndSchemas
        .get(invocationOnMock.getArguments()(0).asInstanceOf[String]).get
    }
  }).when(vertexWrapper).resolveTable(any(classOf[String]))

  doAnswer(new Answer[StructType] {
    override def answer(invocationOnMock: InvocationOnMock): StructType = {
      existingTablesAndSchemas
        .get(invocationOnMock.getArguments()(0).asInstanceOf[String]).get
    }
  }).when(edgeWrapper).resolveTable(any(classOf[String]))

  doAnswer(new Answer[List[Vertex]] {
    override def answer(invocationOnMock: InvocationOnMock): List[Vertex] = {
      oVertices
    }
  }).when(vertexWrapper).genericQuery(any(classOf[String]))

  doAnswer(new Answer[List[Edge]] {
    override def answer(invocationOnMock: InvocationOnMock): List[Edge] = {
      oEdges
    }
  }).when(edgeWrapper).genericQuery(any(classOf[String]))
}