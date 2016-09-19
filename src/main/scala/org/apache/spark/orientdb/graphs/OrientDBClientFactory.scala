package org.apache.spark.orientdb.graphs

import com.tinkerpop.blueprints.impls.orient.{OrientGraphFactory, OrientGraphNoTx}

class OrientDBClientFactory(orientDBCredentials: OrientDBCredentials) extends Serializable {
  private val db: OrientGraphFactory =
    new OrientGraphFactory(orientDBCredentials.dbUrl, orientDBCredentials.username,
      orientDBCredentials.password)
  private var connection: OrientGraphNoTx = _

  def getConnection(): OrientGraphNoTx = {
    connection = db.getNoTx
    connection
  }

  def closeConnection(): Unit = {
    db.close()
  }
}