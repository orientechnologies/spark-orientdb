package org.apache.spark.orientdb

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.spark.orientdb.documents.{OrientDBDocumentWrapper, Parameters}
import org.apache.spark.sql.types.StructType

object MainApplication {
  def getMetadata(): StructType = {
    null
  }

  def createCluster(connection: ODatabaseDocumentTx, cluster: String): Unit = {
    if (!connection.existsCluster(cluster)) {
      connection.addCluster(cluster)
    }
  }

  def createClass(connection: ODatabaseDocumentTx, classname: String, cluster: String, properties: Map[String, OType]): Unit = {
    val schema = connection.getMetadata.getSchema
    if (!schema.existsClass(classname)) {
      val oClass = schema.createClass(classname)

      for ((key, value) <- properties) {
        oClass.createProperty(key, value)
      }
      oClass.addCluster(cluster)
    }
  }

  def create(orientDBDocumentWrapper: OrientDBDocumentWrapper, cluster: String, classname: String): Unit = {
    val oDocument = new ODocument()

    oDocument.field("name", "Vipin", OType.STRING)
    oDocument.field("surname", "V", OType.STRING)

//    oDocument.setClassName(classname)

    orientDBDocumentWrapper.create(cluster, classname, oDocument)
  }

  def read(orientDBDocumentWrapper: OrientDBDocumentWrapper, cluster: String, classname: String, limit: Int): Unit = {
    val oDocuments = orientDBDocumentWrapper.read(cluster, classname, Array("name"), "where surname = 'Dey'")
    println()
  }

  def update(orientDBDocumentWrapper: OrientDBDocumentWrapper, cluster: String, classname: String): Unit = {
    val record = Map("name" -> Tuple2("Subhobrata", OType.STRING), "surname" -> Tuple2("Dey", OType.STRING))
    val filter = Map("name" -> Tuple2(" = ", "Vipin"))

    orientDBDocumentWrapper.update(cluster, classname, record, filter)
    println()
  }

  def delete(orientDBDocumentWrapper: OrientDBDocumentWrapper, cluster: String, classname: String): Unit = {
    val filter = Map("name" -> Tuple2(" = ", "Subhobrata"))
    orientDBDocumentWrapper.delete(cluster, classname, filter)
    println()
  }

  def resolveTable(orientDBDocumentWrapper: OrientDBDocumentWrapper, cluster: String, classname: String): Unit = {
    val results = orientDBDocumentWrapper.resolveTable(cluster, classname)
    println()
  }

  def main(args: Array[String]): Unit = {
    val parameters = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts", "user" -> "root", "password" -> "root", "class" -> "test_orient7__")
    val orientDBDocumentWrapper = new OrientDBDocumentWrapper()
    val connection = orientDBDocumentWrapper.getConnection(Parameters.mergeParameters(parameters))

    // create a cluster
    val cluster = "test_orient7"
    val classname = "test_orient7__"
    val properties = Map("name" -> OType.STRING, "surname" -> OType.STRING)

    createCluster(connection, cluster)
    createClass(connection, classname, cluster, properties)

    create(orientDBDocumentWrapper, cluster, classname)
    read(orientDBDocumentWrapper, cluster, classname, 0)
    update(orientDBDocumentWrapper, cluster, classname)
    read(orientDBDocumentWrapper, cluster, classname, 0)

    resolveTable(orientDBDocumentWrapper, cluster, classname)
    val oDocuments = orientDBDocumentWrapper.genericQuery(s"select count(*) from $classname")
    println(oDocuments.head.field("count"))

//    delete(orientDBDocumentWrapper, cluster, classname)
    read(orientDBDocumentWrapper, cluster, classname, 0)

  }
}