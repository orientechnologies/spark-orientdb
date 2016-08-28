package org.apache.spark.orientdb.documents

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx

class OrientDBClientFactory(orientDBCredentials: OrientDBCredentials) extends Serializable {
  private val db: OPartitionedDatabasePool =
    new OPartitionedDatabasePool(orientDBCredentials.dbUrl, orientDBCredentials.username,
      orientDBCredentials.password)
  private var connection: ODatabaseDocumentTx = _

  def getConnection(): ODatabaseDocumentTx = {
    connection = db.acquire()
    connection
  }

  def closeConnection(): Unit = {
    connection.close()
  }
}