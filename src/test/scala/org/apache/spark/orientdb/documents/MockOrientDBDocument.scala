package org.apache.spark.orientdb.documents

import com.orientechnologies.orient.core.db.ODatabase
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE
import org.mockito.Matchers._
import org.mockito.Mockito._

class MockOrientDBDocument {
/*  val documentWrapper: OrientDBDocumentWrapper = spy(new OrientDBDocumentWrapper(createMockConnection()))

  private def createMockConnection(): ODatabaseDocumentTx = {
    val database = mock(classOf[ODatabase[ORecord]], RETURNS_SMART_NULLS)
    val record = mock(classOf[ORecord], RETURNS_SMART_NULLS)
    val connection = mock(classOf[ODatabaseDocumentTx], RETURNS_SMART_NULLS)
    val documents = new java.util.ArrayList[ODocument]()

    when(connection.begin(TXTYPE.OPTIMISTIC)).thenReturn(connection)
    when(connection.commit()).thenReturn(database)
    when(connection.rollback()).thenReturn(database)
    when(connection.existsCluster(any[String])).thenReturn(false)
    when(connection.save(any[ODocument], any[String])).thenReturn(record)
    when(connection.query(any[OSQLSynchQuery[ODocument]])).thenReturn(documents)
    when(connection.delete(any[ODocument])).thenReturn(connection)

    connection
  } */
}