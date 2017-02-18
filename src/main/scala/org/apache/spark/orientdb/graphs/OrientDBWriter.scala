package org.apache.spark.orientdb.graphs

import java.util

import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.{Edge, Vertex}
import com.tinkerpop.blueprints.impls.orient.{OrientGraphFactory, OrientGraphNoTx}
import org.apache.spark.orientdb.documents.Conversions
import org.apache.spark.orientdb.graphs.Parameters.MergedParameters
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

private[orientdb] class OrientDBVertexWriter(orientDBWrapper: OrientDBGraphVertexWrapper,
                                             orientDBClientFactory: OrientDBCredentials => OrientDBClientFactory)
                  extends Serializable {

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

      if (field.name == "id") {
        createdVertexType.createIndex(s"${vertexType}Idx", OClass.INDEX_TYPE.UNIQUE, field.name)
      }
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

          val fields = row.schema.fields

          val fieldNames = fields.map(_.name)
          if (!fieldNames.contains("id")) {
            throw new IllegalArgumentException("'id' is a mandatory parameter " +
              "for creating a vertex")
          }
          val key = row.getAs[Object](fieldNames.indexOf("id"))
          val properties = new util.HashMap[String, Object]()
          properties.put("id", key)
          connection.setStandardElementConstraints(false)
          val createdVertex = connection.addVertex(s"class:${params.vertexType.get}", properties)

          var count = 0
          while (count < fields.length) {
            val sparkType = fields(count).dataType
            val orientDBType = Conversions
              .sparkDTtoOrientDBDT(sparkType)
            createdVertex.setProperty(fields(count).name,
              row.getAs[sparkType.type](count), orientDBType)

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

    val connector = orientDBWrapper.getConnection(params)
    val createdEdgeType = connector.createEdgeType(edgeType)
    if (!params.lightWeightEdge) {
      dfSchema.foreach(field => {
        createdEdgeType.createProperty(field.name, Conversions.sparkDTtoOrientDBDT(field.dataType))
      })
    }
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

    val (inVertexType, outVertexType) = params.vertexType match {
      case Some(vertexTypeNames) =>
        val cols = vertexTypeNames.split(",")
        if (cols.length == 1) (cols(0), cols(0))
        else if (cols.length == 2) (cols(0), cols(1))
        else throw new IllegalArgumentException("More than 2 'vertextype' specified")
      case None =>
        throw new IllegalArgumentException("Saving edges also require a vertex type specified by " +
          "'vertextype' parameter")
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
            inVertexName = row.getAs[Object](fields.map(_.name).indexOf("src")).toString
          } catch {
            case e: Exception => throw new IllegalArgumentException("'src' is a mandatory parameter " +
              "for creating an edge")
          }

          var outVertexName: String = null
          try {
            outVertexName = row.getAs[Object](fields.map(_.name).indexOf("dst")).toString
          } catch {
            case e: Exception => throw new IllegalArgumentException("'dst' is a mandatory parameters " +
              "for creating an edge")
          }

          val inVertices: List[Vertex] = connection
            .command(new OCommandSQL(s"select * from $inVertexType where id = '$inVertexName'"))
            .execute()
            .asInstanceOf[java.lang.Iterable[Vertex]]
            .toList

          val inVertex: Option[Vertex] =
            if (inVertices.isEmpty && params.createVertexIfNotExist) {
              // log.info(s"in Vertex $inVertexName does not exist. Creating it...")
              val v = connection.addVertex(inVertexType, null)
              v.setProperty("id", inVertexName)
              Some(v)
            } else {
              inVertices.headOption
            }

          val outVertices: List[Vertex] = connection
            .command(new OCommandSQL(s"select * from $outVertexType where id = '$outVertexName'"))
            .execute()
            .asInstanceOf[java.lang.Iterable[Vertex]]
            .toList

          val outVertex: Option[Vertex] =
            if (outVertices.isEmpty && params.createVertexIfNotExist) {
              // log.info(s"out Vertex $outVertexName does not exist. Creating it...")
              val v = connection.addVertex(outVertexType, null)
              v.setProperty("id", outVertexName)
              Some(v)
            } else {
              outVertices.headOption
            }

          for {
            in <- inVertex
            out <- outVertex
          } {
            val createdEdge = connection.addEdge(null, in, out, params.edgeType.get)
            for (i <- fields.indices) {
              val sparkType = fields(i).dataType
              val orientDBType = Conversions.sparkDTtoOrientDBDT(sparkType)
              createdEdge.setProperty(fields(i).name, row.getAs[sparkType.type](i), orientDBType)
            }
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
