package org.apache.spark.orientdb.graphs

import com.google.common.collect.Lists
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.{Edge, Vertex}
import com.tinkerpop.blueprints.impls.orient.{OrientGraphFactory, OrientGraphNoTx}
import org.apache.spark.orientdb.graphs.Parameters.MergedParameters
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

class OrientDBGraphWrapper extends Serializable {
  protected var connectionPool: Option[OrientGraphFactory] = None
  protected var connection: OrientGraphNoTx = _

  /**
    * Get instance of Database Connection
    * @return Database connection instance
    */
  def getConnection(params: MergedParameters): OrientGraphNoTx = {
    try {
      if (connectionPool.isEmpty) {
        connectionPool = Some(new OrientGraphFactory(
          params.dbUrl.get,
          params.credentials.get._1,
          params.credentials.get._2
        ))
      }
      connection = connectionPool.get.getNoTx
      connection
    } catch {
      case e: Exception => throw new RuntimeException(s"Connection Exception Occurred: ${e.getMessage}")
    }
  }

  protected def getCatalystType(oType: OType): DataType = {
    val dataType = oType match {
      case OType.BOOLEAN => BooleanType
      case OType.INTEGER => IntegerType
      case OType.SHORT => ShortType
      case OType.LONG => LongType
      case OType.FLOAT => FloatType
      case OType.DOUBLE => DoubleType
      case OType.DATETIME => TimestampType
      case OType.STRING => StringType
      case OType.BINARY => BinaryType
      case OType.BYTE => ByteType
      case OType.DATE => DateType
      case OType.DECIMAL => DecimalType(38, 18)
      case OType.ANY => null
    }
    dataType
  }
}

class OrientDBGraphVertexWrapper extends OrientDBGraphWrapper {
  /**
    * Check if vertex Type already exists in OrientDB
    * @param vertexType vertex Type name in OrientDB
    * @return true/false
    */
  def doesVertexTypeExists(vertexType: String): Boolean = {
    connection.getVertexType(vertexType) != null
  }

  /**
    * Create API
    * @param vertexType vertex Type in OrientDB graph
    */
  def create(vertexType: String, properties: Map[String, Object]): Boolean = {
    try {
      val vertex = connection.addVertex(s"$vertexType", null)
      if (vertex != null) {
        properties.foreach(property => vertex.setProperty(property._1, property._2))
        true
      }
      else false
    } catch {
      case e: Exception => throw new RuntimeException(s"An exception was thrown: ${e.getMessage}")
    }
  }

  /**
    * Read API
    * @param vertexType vertex Type in OrientDB graph
    * @param requiredProperties required properties in every vertex
    * @param filters filter conditions for retrieving records
    * @return
    */
  def read(vertexType: String, requiredProperties: Array[String],
           filters: String, query: String = null): List[Vertex] = {
    var vertices: java.util.List[Vertex] = null
    val columns = requiredProperties.mkString(", ")

    if (query == null) {
      vertices = Lists.newArrayList(
        connection.command(new OCommandSQL(s"select $columns from $vertexType $filters"))
          .execute().asInstanceOf[java.lang.Iterable[Vertex]])
    } else {
      var queryStr = ""

      if (filters != "") {
        if (query.contains("WHERE ")) {
          val parts = query.split("WHERE ")

          if (parts.size > 1) {
            val firstpart = parts(0)
            val secondpart = parts(1)

            queryStr = s"$firstpart $filters and $secondpart"
          } else {
            queryStr = s"$query $filters"
          }
        } else if (query.contains("where ")) {
          val parts = query.split("WHERE ")

          if (parts.size > 1) {
            val firstpart = parts(0)
            val secondpart = parts(1)

            queryStr = s"$firstpart $filters and $secondpart"
          } else {
            queryStr = s"$query $filters"
          }
        } else {
          queryStr = s"$query $filters"
        }
      } else {
        queryStr = query
      }
      println(queryStr)
      vertices = Lists.newArrayList[Vertex](
        connection.command(new OCommandSQL(queryStr))
          .execute().asInstanceOf[java.lang.Iterable[Vertex]])
    }
    vertices.toList
  }

  /**
    * Bulk Create API
    * @param vertexTypes vertex type names in Orient DB graph
    * @return
    */
  def bulkCreate(vertexTypes: List[String], properties: List[Map[String, Object]]): Boolean = {
    try {
      var count = 0
      vertexTypes.foreach(vertexType => {
        create(vertexType, properties(count))
        count += 1
      })
      true
    } catch {
      case e: Exception => throw new RuntimeException(s"An exception was thrown: ${e.getMessage}")
    }
  }

  /**
    * Delete API
    * @param vertexType vertex Type in Orient DB Graph
    * @param filter filter which filters records to be deleted in the format (fieldName, (filterOperator, fieldValue))
    */
  def delete(vertexType: String, filter: Map[String, Tuple2[String, String]]): Boolean = {
    try {
      var filterStr = ""

      if (filter != null) {
        val filterLength = filter.size
        var count = 1
        filter.foreach(p => {
          if (count == filterLength) {
            filterStr += p._1 + " " + p._2._1 + " '" + p._2._2 + "'"
          } else {
            filterStr += p._1 + " " + p._2._1 + " '" + p._2._2 + "' and"
          }
          count += 1
        })
      }

      var verticesToBeDeleted: java.util.List[Vertex] = null

      if (filterStr != "") {
        verticesToBeDeleted = Lists.newArrayList[Vertex](connection
          .command(new OCommandSQL(s"select * from $vertexType where $filterStr"))
          .execute().asInstanceOf[java.lang.Iterable[Vertex]])
      } else {
        verticesToBeDeleted = Lists.newArrayList[Vertex](connection
          .command(new OCommandSQL(s"select * from $vertexType"))
          .execute().asInstanceOf[java.lang.Iterable[Vertex]])
      }

      verticesToBeDeleted.foreach(vertexToBeDeleted => {
        connection.removeVertex(vertexToBeDeleted)
      })
      true
    } catch {
      case e: Exception => throw new RuntimeException(s"An exception was thrown: ${e.getMessage}")
    }
  }

  /**
    * Resolve Orient DB Vertex Type metadata
    * @param vertexType vertex type name in OrientDB Graph
    * @return
    */
  def resolveTable(vertexType: String): StructType = {
    val vertexTypeClass = connection.getVertexType(vertexType)

    if (vertexTypeClass == null) {
      throw new RuntimeException(s"The Vertex type $vertexType does not exist in Orient DB Graph")
    }

    val properties = vertexTypeClass.properties()
    val ncols = properties.size()
    val fields = new Array[StructField](ncols)
    val iterator = properties.iterator()

    var i = 0
    while (iterator.hasNext) {
      val property = iterator.next()
      val columnName = property.getName

      // there are no keys..hence for now every field is nullable
      val nullable = true
      val columnType = getCatalystType(property.getType)
      fields(i) = StructField(columnName, columnType, nullable)
      i = i + 1
    }
    new StructType(fields)
  }

  /**
    * execute generic query on OrientDB Graph vertices
    */
  def genericQuery(query: String): List[Vertex] = {
    val vertices: java.util.List[Vertex] = Lists.newArrayList[Vertex](connection.command(
      new OCommandSQL(query)).execute().asInstanceOf[java.lang.Iterable[Vertex]])
    vertices.toList
  }
}

private[orientdb] object DefaultOrientDBGraphVertexWrapper extends OrientDBGraphVertexWrapper

class OrientDBGraphEdgeWrapper extends OrientDBGraphWrapper {
  /**
    * Check if edge Type already exists in OrientDB
    * @param edgeType edge Type name in OrientDB
    * @return true/false
    */
  def doesEdgeTypeExists(edgeType: String): Boolean = {
    connection.getEdgeType(edgeType) != null
  }

  /**
    * Create API
    * @param edgeType edge Type in OrientDB graph
    */
  def create(edgeType: String, inVertex: Vertex,
             outVertex: Vertex, properties: Map[String, Object]): Boolean = {
    try {
      val edge = connection.addEdge(null, inVertex, outVertex, edgeType)

      if (edge == null) false
      else {
        properties.foreach(property =>
          edge.setProperty(property._1, property._2))
        true
      }
    } catch {
      case e: Exception => throw new RuntimeException(s"An exception was thrown: ${e.getMessage}")
    }
  }

  /**
    * Read API
    * @param edgeType edge Type in OrientDB graph
    * @param requiredProperties required properties in every vertex
    * @param filters filter conditions for retrieving records
    * @return
    */
  def read(edgeType: String, requiredProperties: Array[String],
           filters: String, query: String = null): List[Edge] = {
    var edges: java.util.List[Edge] = null
    val columns = requiredProperties.mkString(", ")

    if (query == null) {
      edges = Lists.newArrayList(
        connection.command(new OCommandSQL(s"select * from $edgeType $filters"))
          .execute().asInstanceOf[java.lang.Iterable[Edge]])
    } else {
      var queryStr = ""

      if (filters != "") {
        if (query.contains("WHERE ")) {
          val parts = query.split("WHERE ")

          if (parts.size > 1) {
            val firstpart = parts(0)
            val secondpart = parts(1)

            queryStr = s"$firstpart $filters and $secondpart"
          } else {
            queryStr = s"$query $filters"
          }
        } else if (query.contains("where ")) {
          val parts = query.split("WHERE ")

          if (parts.size > 1) {
            val firstpart = parts(0)
            val secondpart = parts(1)

            queryStr = s"$firstpart $filters and $secondpart"
          } else {
            queryStr = s"$query $filters"
          }
        } else {
          queryStr = s"$query $filters"
        }
      } else {
        queryStr = query
      }
      println(queryStr)
      edges = Lists.newArrayList(
        connection.command(new OCommandSQL(queryStr))
          .execute().asInstanceOf[java.lang.Iterable[Edge]])
    }
    edges.toList
  }

  /**
    * Bulk Create API
    * @param edgeTypes edge type names in Orient DB graph.
    *                  in the format Map((edgeType, (inVertex, outVertex)))
    * @return
    */
  def bulkCreate(edgeTypes: Map[String, Tuple2[Vertex, Vertex]],
                 properties: List[Map[String, Object]]): Boolean = {
    try {
      var count = 0
      edgeTypes.foreach(edgeType => {
        create(edgeType._1, edgeType._2._1,
          edgeType._2._1, properties(count))
        count += 1
      })
      true
    } catch {
      case e: Exception => throw new RuntimeException(s"An exception was thrown: ${e.getMessage}")
    }
  }

  /**
    * Delete API
    * @param edgeType edge Type in Orient DB Graph
    * @param filter filter which filters records to be deleted in the format (fieldName, (filterOperator, fieldValue))
    */
  def delete(edgeType: String, filter: Map[String, Tuple2[String, String]]): Boolean = {
    try {
      var filterStr = ""

      if (filter != null) {
        val filterLength = filter.size
        var count = 1
        filter.foreach(p => {
          if (count == filterLength) {
            filterStr += p._1 + " " + p._2._1 + " '" + p._2._2 + "'"
          } else {
            filterStr += p._1 + " " + p._2._1 + " '" + p._2._2 + "' and"
          }
          count += 1
        })
      }

      var edgesToBeDeleted: java.util.List[Edge] = null

      if (filterStr != "") {
        edgesToBeDeleted = Lists.newArrayList(
          connection.command(new OCommandSQL(
            s"select * from $edgeType where $filterStr"))
            .execute().asInstanceOf[java.lang.Iterable[Edge]])
      } else {
        edgesToBeDeleted = Lists.newArrayList(
          connection.command(new OCommandSQL(
        s"select * from $edgeType")).execute()
            .asInstanceOf[java.lang.Iterable[Edge]])
      }

      edgesToBeDeleted.foreach(edgeToBeDeleted => {
        connection.removeEdge(edgeToBeDeleted)
      })
      true
    } catch {
      case e: Exception => throw new RuntimeException(s"An exception was thrown: ${e.getMessage}")
    }
  }

  /**
    * Resolve Orient DB Edge Type metadata
    * @param edgeType edge type name in OrientDB Graph
    * @return
    */
  def resolveTable(edgeType: String): StructType = {
    val edgeTypeClass = connection.getEdgeType(edgeType)

    if (edgeTypeClass == null) {
      throw new RuntimeException(s"The edge type $edgeType does not exist in Orient DB Graph")
    }

    val properties = edgeTypeClass.properties()
    val ncols = properties.size()
    val fields = new Array[StructField](ncols)
    val iterator = properties.iterator()

    var i = 0
    while (iterator.hasNext) {
      val property = iterator.next()
      val columnName = property.getName

      // there are no keys..hence for now every field is nullable
      val nullable = false
      val columnType = getCatalystType(property.getType)
      fields(i) = StructField(columnName, columnType, nullable)
      i = i + 1
    }
    new StructType(fields)
  }

  /**
    * execute generic query on OrientDB Graph edges
    */
  def genericQuery(query: String): List[Edge] = {
    val edges: java.util.List[Edge] = Lists.newArrayList(connection.command(
      new OCommandSQL(query)).execute().asInstanceOf[java.lang.Iterable[Edge]])
    edges.toList
  }
}

private[orientdb] object DefaultOrientDBGraphEdgeWrapper extends OrientDBGraphEdgeWrapper