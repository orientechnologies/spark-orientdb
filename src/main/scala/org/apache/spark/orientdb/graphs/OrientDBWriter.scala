package org.apache.spark.orientdb.graphs

import com.orientechnologies.orient.core.id.ORecordId
import com.tinkerpop.blueprints.impls.orient.{OrientGraphFactory, OrientGraphNoTx}
import org.apache.spark.orientdb.documents.Conversions
import org.apache.spark.orientdb.graphs.Parameters.MergedParameters
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

private[orientdb] class OrientDBVertexWriter(orientDBWrapper: OrientDBGraphVertexWrapper,
                                             orientDBClientFactory: OrientDBCredentials => OrientDBClientFactory)
                  extends Serializable {
  private val log = LoggerFactory.getLogger(getClass)

  private[orientdb] def createOrientDBVertex(data: DataFrame, params: MergedParameters): Unit = {
    val dfSchema = data.schema
    val vertexType = params.vertexType match {
      case Some(vertexTypeName) => vertexTypeName
      case None => throw new IllegalArgumentException("For save operations you must specify a OrientDB Vertex Type" +
        " with the 'vertextype' parameter")
    }
    var cluster = params.cluster match {
      case Some(clusterName) => clusterName
      case None => null
    }

    val connector = orientDBWrapper.getConnection(params)
    val createdVertexType = connector.createVertexType(vertexType)

    dfSchema.foreach(field => {
      createdVertexType.createProperty(field.name, Conversions.sparkDTtoOrientDBDT(field.dataType))
    })
  }

  private[orientdb] def dropOrientDBVertex(params: MergedParameters): Unit = {
    val connection = orientDBWrapper.getConnection(params)

    val vertexType = params.vertexType
    if (vertexType.isEmpty) {
      throw new IllegalArgumentException("For save operations you must specify a OrientDB Vertex Type" +
        " with the 'vertextype' parameter")
    }

    if (connection.getVertexType(vertexType.get) != null) {
      orientDBWrapper.delete(vertexType.get, null)
      connection.dropVertexType(vertexType.get)
    }
  }

  private def doOrientDBVertexLoad(connection: OrientGraphNoTx,
                                   data: DataFrame,
                                   params: MergedParameters): Unit = {
    val vertexType = params.vertexType
    if (vertexType.isEmpty) {
      throw new IllegalArgumentException("For save operations you must specify a OrientDB Vertex Type" +
        " with the 'vertextype' parameter")
    }

    if (connection.getVertexType(vertexType.get) == null) {
      createOrientDBVertex(data, params)
    }

    try {
      data.foreachPartition(rows => {
        val graphFactory = new OrientGraphFactory(params.dbUrl.get,
                                        params.credentials.get._1,
                                        params.credentials.get._2)
        val connection = graphFactory.getNoTx

        while (rows.hasNext) {
          val row = rows.next()
          val createdVertex = connection.addVertex(params.vertexType.get, null)

          val fields = row.schema.fields
          var count = 0
          while (count < fields.length) {
            val sparkType = fields(count).dataType
            val orientDBType = Conversions
              .sparkDTtoOrientDBDT(sparkType)
            createdVertex.setProperty(fields(count).name,
                                      row.getAs[sparkType.type](count),
                                      orientDBType)

            count = count + 1
          }
        }
        graphFactory.close()
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
        dropOrientDBVertex(params)
      }
      doOrientDBVertexLoad(connection, data, params)
    } finally {
      orientDBWrapper.close()
    }
  }
}

object DefaultOrientDBVertexWriter extends OrientDBVertexWriter(
            DefaultOrientDBGraphVertexWrapper,
            orientDBCredentials => new OrientDBClientFactory(orientDBCredentials))

private[orientdb] class OrientDBEdgeWriter(orientDBWrapper: OrientDBGraphEdgeWrapper,
                                           orientDBClientFactory: OrientDBCredentials => OrientDBClientFactory)
                  extends Serializable {
  private val log = LoggerFactory.getLogger(getClass)

  private[orientdb] def createOrientDBEdge(data: DataFrame, params: MergedParameters): Unit = {
    val dfSchema = data.schema
    val edgeType = params.edgeType match {
      case Some(edgeTypeName) => edgeTypeName
      case None => throw new IllegalArgumentException("For save operations you must specify a OrientDB Edge Type" +
        " with the 'edgetype' parameter")
    }

    var cluster = params.cluster match {
      case Some(clusterName) => clusterName
      case None => null
    }

    val connector = orientDBWrapper.getConnection(params)
    val createdEdgeType = connector.createEdgeType(edgeType)

    dfSchema.foreach(field => {
      if (field.name != "src" && field.name != "dst") {
        createdEdgeType.createProperty(field.name,
          Conversions.sparkDTtoOrientDBDT(field.dataType))
      }
    })
  }

  private[orientdb] def dropOrientDBEdge(params: MergedParameters): Unit = {
    val connection = orientDBWrapper.getConnection(params)

    val edgeType = params.edgeType
    if (edgeType.isEmpty) {
      throw new IllegalArgumentException("For save operations you must specify a OrientDB Edge Type" +
        " with the 'edgetype' parameter")
    }

    if (connection.getEdgeType(edgeType.get) != null) {
      orientDBWrapper.delete(edgeType.get, null)
      connection.dropEdgeType(edgeType.get)
    }
  }

  private def doOrientDBEdgeLoad(connection: OrientGraphNoTx,
                                   data: DataFrame,
                                   params: MergedParameters): Unit = {
    val edgeType = params.edgeType
    if (edgeType.isEmpty) {
      throw new IllegalArgumentException("For save operations you must specify a OrientDB Edge Type" +
        " with the 'edgetype' parameter")
    }

    if (connection.getEdgeType(edgeType.get) == null) {
      createOrientDBEdge(data, params)
    }

    try {
      data.foreachPartition(rows => {
        val graphFactory = new OrientGraphFactory(params.dbUrl.get,
                                                  params.credentials.get._1,
                                                  params.credentials.get._2)
        val connection = graphFactory.getNoTx

        while (rows.hasNext) {
          val row = rows.next()

          val fields = row.schema.fields

          var inVertexName: String = null
          try {
            inVertexName = fields.filter(field => field.name == "src")
              .toList.head.name
          } catch {
            case e: Exception => throw new IllegalArgumentException("'src' is a mandatory parameter " +
              "for creating an edge")
          }

          var outVertexName: String = null
          try {
            outVertexName = fields.filter(field => field.name == "dst")
              .toList.head.name
          } catch {
            case e: Exception => throw new IllegalArgumentException("'dst' is a mandatory parameters " +
              "for creating an edge")
          }

          var inVertex = connection.getVertex(inVertexName)
          if (inVertex == null) {
            println(s"in Vertex $inVertexName does not exist. Creating it...")
            inVertex = connection.addVertex(inVertexName, null)
          }

          var outVertex = connection.getVertex(outVertexName)
          if (outVertex == null) {
            println(s"out Vertex $outVertexName does not exist. Creating it...")
            outVertex = connection.addVertex(outVertexName, null)
          }

          val createdEdge = connection.addEdge(null, inVertex,
            outVertex, params.edgeType.get)

          var count = 0
          while (count < fields.length) {
            if (fields(count).name != "src" &&
                fields(count).name != "dst") {
              val sparkType = fields(count).dataType
              val orientDBType = Conversions
                .sparkDTtoOrientDBDT(sparkType)
              createdEdge.setProperty(fields(count).name,
                                      row.getAs[sparkType.type](count),
                                      orientDBType)
            }
            count = count + 1
          }
          graphFactory.close()
        }
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
        dropOrientDBEdge(params)
      }
      doOrientDBEdgeLoad(connection, data, params)
    } finally {
      orientDBWrapper.close()
    }
  }
}

object DefaultOrientDBEdgeWriter extends OrientDBEdgeWriter(
                  DefaultOrientDBGraphEdgeWrapper,
                  orientDBCredentials => new OrientDBClientFactory(orientDBCredentials))