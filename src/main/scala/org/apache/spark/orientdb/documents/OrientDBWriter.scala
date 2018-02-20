package org.apache.spark.orientdb.documents

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import Parameters.MergedParameters
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

private[orientdb] class OrientDBWriter(orientDBWrapper: OrientDBDocumentWrapper,
                                       orientDBClientFactory: OrientDBCredentials => OrientDBClientFactory)
                  extends Serializable {
  private val log = LoggerFactory.getLogger(getClass)

  private[orientdb] def createOrientDBClass(data: DataFrame, params: MergedParameters): Unit = {
    val dfSchema = data.schema
    val classname = params.className match {
      case Some(className) => className
      case None => throw new IllegalArgumentException("For save operations you must specify a OrientDB Class " +
        "name with the 'classname' parameter")
    }
    var clusters = params.clusterNames match {
      case Some(clusterNames) => clusterNames
      case None => null
    }

    val connector = orientDBWrapper.getConnection(params)
    val schema = connector.getMetadata.getSchema
    val createdClass = schema.createClass(classname)

    dfSchema.foreach(field => {
      createdClass.createProperty(field.name, Conversions.sparkDTtoOrientDBDT(field.dataType))
    })

    if (clusters != null) {
      clusters.foreach(cluster => createdClass.addCluster(cluster))
    } else {
      clusters = List(connector.getClusterNameById(createdClass.getDefaultClusterId))
    }
  }

  private[orientdb] def dropOrientDBClass(params: MergedParameters): Unit = {
    val connection = orientDBWrapper.getConnection(params)

    // create class if not exists
    val classname = params.className
    if (classname.isEmpty) {
      throw new IllegalArgumentException("For save operations you must specify a OrientDB Class " +
        "name with the 'classname' parameter")
    }
    var cluster = params.clusterNames

    // Todo use Future
    if (connection.getMetadata.getSchema.existsClass(classname.get)) {
      orientDBWrapper.delete(null, classname.get, null)

      val schema = connection.getMetadata.getSchema
      schema.dropClass(classname.get)
    }
  }

  private def doOrientDBLoad(connection: ODatabaseDocumentTx,
                             data: DataFrame,
                             params: MergedParameters): Unit = {
    // create class if not exists
    val classname = params.className
    if (classname.isEmpty) {
      throw new IllegalArgumentException("For save operations you must specify a OrientDB Class " +
        "name with the 'classname' parameter")
    }

    val schema = connection.getMetadata.getSchema

    if (!schema.existsClass(classname.get)) {
      createOrientDBClass(data, params)
    }

    var clusters = params.clusterNames
    if (clusters.isEmpty) {
      val schema = connection.getMetadata.getSchema
      val currClass = schema.getClass(classname.get)
      clusters = Some(List(connection.getClusterNameById(currClass.getDefaultClusterId)))
    }

    // Todo use future
    // load data into Orient DB
    try {
      data.foreachPartition(rows => {
        val connection = new ODatabaseDocumentTx(params.dbUrl.get)
        connection.open(params.credentials.get._1, params.credentials.get._2)
        val rowsList = rows.toList
        val rowsPerCluster = rowsList.length % clusters.get.length match {
          case 0 => rowsList.length / clusters.get.length
          case _ => (rowsList.length / clusters.get.length) + 1
        }

        var countPerCluster = 0
        var clusterIdx = 0
        rowsList.foreach { row =>
          connection.save(Conversions.convertRowsToODocuments(row), clusters.get(clusterIdx))
          countPerCluster = countPerCluster + 1

          if (countPerCluster % rowsPerCluster == 0) {
            countPerCluster = 0
            clusterIdx = clusterIdx + 1
          }
        }

        connection.close()
      })
    } catch {
      case e: Exception =>
        throw new RuntimeException("An exception was thrown: " + e.getMessage)
    }
  }

  def saveToOrientDB(data: DataFrame, saveMode: SaveMode, params: MergedParameters): Unit = {
    val connection = orientDBWrapper.getConnection(params)
    try {
      if (saveMode == SaveMode.Overwrite) {
        dropOrientDBClass(params)
      }
      doOrientDBLoad(connection, data, params)
    } finally {
      connection.close()
    }
  }
}

object DefaultOrientDBWriter extends OrientDBWriter(
  DefaultOrientDBDocumentWrapper,
  orientDBCredemtials => new OrientDBClientFactory(orientDBCredemtials))