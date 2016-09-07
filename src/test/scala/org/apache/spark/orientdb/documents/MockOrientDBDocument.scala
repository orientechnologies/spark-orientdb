package org.apache.spark.orientdb.documents

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.spark.orientdb.documents.Parameters.MergedParameters
import org.apache.spark.sql.types.StructType
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

class MockOrientDBDocument(existingTablesAndSchemas: Map[String, StructType],
                           oDocuments: List[ODocument]) {
  val documentWrapper: OrientDBDocumentWrapper = spy(new OrientDBDocumentWrapper())

  doAnswer(new Answer[ODatabaseDocumentTx] {
    override def answer(invocationOnMock: InvocationOnMock): ODatabaseDocumentTx = {
      mock(classOf[ODatabaseDocumentTx], RETURNS_SMART_NULLS)
    }
  }).when(documentWrapper).getConnection(any(classOf[MergedParameters]))

  doAnswer(new Answer[Boolean] {
    override def answer(invocationOnMock: InvocationOnMock): Boolean = {
      existingTablesAndSchemas.contains(invocationOnMock.getArguments()(1).asInstanceOf[String])
    }
  }).when(documentWrapper).doesClassExists(any(classOf[String]))

  doAnswer(new Answer[Boolean] {
    override def answer(invocationOnMock: InvocationOnMock): Boolean = {
      true
    }
  }).when(documentWrapper).create(any(classOf[String]), any(classOf[String]),
    any(classOf[ODocument]))

  doAnswer(new Answer[List[ODocument]] {
    override def answer(invocationOnMock: InvocationOnMock): List[ODocument] = {
      oDocuments
    }
  }).when(documentWrapper).read(any(classOf[String]), any(classOf[String]), any(classOf[Array[String]]),
    any(classOf[String]), any(classOf[String]))

  doAnswer(new Answer[Boolean] {
    override def answer(invocationOnMock: InvocationOnMock): Boolean = {
      true
    }
  }).when(documentWrapper).delete(any(classOf[String]), any(classOf[String]),
    any(classOf[Map[String, Tuple2[String, String]]]))

  doAnswer(new Answer[StructType] {
    override def answer(invocationOnMock: InvocationOnMock): StructType = {
      existingTablesAndSchemas.get(invocationOnMock.getArguments()(1).asInstanceOf[String]).get
    }
  }).when(documentWrapper).resolveTable(any(classOf[String]), any(classOf[String]))

  doAnswer(new Answer[List[ODocument]] {
    override def answer(invocationOnMock: InvocationOnMock): List[ODocument] = {
      oDocuments
    }
  }).when(documentWrapper).genericQuery(any(classOf[String]))
}