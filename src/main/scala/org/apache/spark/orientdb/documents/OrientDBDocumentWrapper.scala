package org.apache.spark.orientdb.documents

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE
import org.apache.spark.orientdb.documents.Parameters.MergedParameters
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

class OrientDBDocumentWrapper extends Serializable {
  private var connectionPool: Option[OrientDBClientFactory] = None
  private var connection: ODatabaseDocumentTx = _

  def openTransaction(): Unit = {
    connection.begin(TXTYPE.OPTIMISTIC)
  }

  def commitTransaction(): Unit = {
    connection.commit()
  }

  def rollbackTransaction(): Unit = {
    connection.rollback()
  }

  /**
    * Get instance of Database connection
    * @return Database connection instance
    */
  def getConnection(params: MergedParameters): ODatabaseDocumentTx = {
    try {
      if (connectionPool.isEmpty) {
        connectionPool = Some(new OrientDBClientFactory(new OrientDBCredentials {
          this.dbUrl = params.dbUrl.get
          this.username = params.credentials.get._1
          this.password = params.credentials.get._2
        }))
      }
      connection = connectionPool.get.getConnection()
      connection
    } catch {
      case e: Exception => throw new RuntimeException(s"Connection Exception Occurred: ${e.getMessage}")
    }
  }

  /**
    * Check if class already exists in Orient DB
    * @param classname cluster name in OrientDB
    * @return true/false
    */
  def doesClassExists(classname: String): Boolean = {
    val schema = connection.getMetadata.getSchema
    schema.existsClass(classname)
  }

  /**
    * Create API
    * @param cluster cluster name in OrientDB
    * @param classname class name in OrientDB
    * @param document document to be created
    */
  def create(cluster: String, classname: String, document: ODocument): Boolean = {
    if (connection.save(document, cluster) != null)
      return true
    false
  }

  /**
    * Read API
    * @param cluster cluster name in OrientDB
    * @param classname class name in OrientDB
    * @param filters filters the no. of records retrieved
    * @return
    */
  def read(cluster: String, classname: String, requiredColumns: Array[String],
           filters: String, query: String = null): List[ODocument] = {
    var documents: java.util.List[ODocument] = null
    val columns = requiredColumns.mkString(", ")

    if (query == null) {
      documents = connection.query(
        new OSQLSynchQuery[ODocument]
        (s"select $columns from $classname $filters"))
    } else {
      var queryStr = ""

      if (filters != "") {
        if (query.contains("WHERE ")) {
          val parts = query.split("WHERE ")

          if ( parts.size > 1) {
            val firstpart = parts(0)
            val secondpart = parts(1)

            queryStr = s"$firstpart $filters and $secondpart"
          } else {
            queryStr = s"$query $filters"
          }
        } else if (query.contains("where ")) {
          val parts = query.split("where ")

          if ( parts.size > 1) {
            val firstpart = parts(0)
            val secondpart = parts(1)

            queryStr = s"$firstpart $filters and $secondpart"
          } else {
            queryStr = s"$query $filters"
          }
        } else {
          queryStr = s"$query $filters"
        }
      } else queryStr = query
      documents = connection.query(new OSQLSynchQuery[ODocument](queryStr))
    }

    documents.toList
  }

  /**
    * Bulk Create API
    * @param cluster cluster name in OrientDB
    * @param classname class name in OrientDB
    * @param documents List of documents to be created
    */
  def bulkCreate(cluster: String, classname: String, documents: List[ODocument]): Boolean = {
    try {
      openTransaction()
      documents.foreach(document => {
        if (!create(cluster, classname, document)) {
          rollbackTransaction()
          return false
        }

      })
      commitTransaction()
      true
    } catch {
      case e: Exception =>
        rollbackTransaction()
        throw new RuntimeException("An exception was thrown: " + e.getMessage)
    }
  }

  /**
    * Update API
    * @param cluster cluster name in OrientDB
    * @param classname class name in OrientDB
    * @param record new record to be updated in the format (fieldName, (fieldValue, fieldType))
    * @param filter filter which filters records to be updated in the format (fieldName, (filterOperator, fieldValue))
    */
  def update(cluster: String, classname: String, record: Map[String, Tuple2[String, OType]],
             filter: Map[String, Tuple2[String, String]]): Boolean = {
    var filterStr = ""

    val filterLength = filter.size
    var count = 1
    filter.foreach(p => {
      if (count == filterLength)
      // handle int s, only strings are handled
        filterStr += p._1 + " " + p._2._1 + " '" + p._2._2 + "'"
      else
        filterStr += p._1 + " " + p._2._1 + " '" + p._2._2 + "' and "
      count += 1
    })

    val documentsToBeUpdated: java.util.List[ODocument] = connection.query(
      new OSQLSynchQuery[ODocument]("select * from " + classname + " where " + filterStr))

    try {
      openTransaction()
      documentsToBeUpdated.foreach(document => {
        record.foreach(field => {
          document.field(field._1, field._2._1, field._2._2)
        })
        if (!create(cluster, classname, document)) {
          rollbackTransaction()
          return false
        }
      })
      commitTransaction()
      true
    } catch {
      case e: Exception =>
        rollbackTransaction()
        throw new RuntimeException("An exception was thrown: " + e.getMessage)
    }
  }

  /**
    * Delete API
    * @param cluster cluster name in OrientDB
    * @param classname class name in OrientDB
    * @param filter filter which filters records to be deleted in the format (fieldName, (filterOperator, fieldValue))
    */
  def delete(cluster: String, classname: String, filter: Map[String, Tuple2[String, String]]): Boolean = {
    try {
      var filterStr = ""

      if (filter != null) {
        val filterLength = filter.size
        var count = 1
        filter.foreach(p => {
          if (count == filterLength)
          // handle int s, only strings are handled
            filterStr += p._1 + " " + p._2._1 + " '" + p._2._2 + "'"
          else
            filterStr += p._1 + " " + p._2._1 + " '" + p._2._2 + "' and "
          count += 1
        })
      }

      var documentsToBeDeleted: java.util.List[ODocument] = null
      if (filterStr != "") {
        documentsToBeDeleted = connection.query(
          new OSQLSynchQuery[ODocument]("select * from " + classname + " where " + filterStr))
      } else {
        documentsToBeDeleted = connection.query(
          new OSQLSynchQuery[ODocument]("select * from " + classname))
      }

      openTransaction()
      documentsToBeDeleted.foreach(document => {
        if (connection.delete(document) == null) {
          rollbackTransaction()
          return false
        }
      })
      commitTransaction()
      true
    } catch {
      case e: Exception =>
        rollbackTransaction()
        throw new RuntimeException("An exception was thrown: " + e.getMessage)
    }
  }

  /**
    * Resolve OrientDB class metadata
    * @param cluster cluster name in OrientDB
    * @param classname class name in OrientDB
    * @return
    */
  def resolveTable(cluster: String, classname: String): StructType = {
    val oClass = connection.getMetadata.getSchema.getClass(classname)

    val properties = oClass.properties()
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
    * execute generic query on OrientDB
    */
  def genericQuery(query: String): List[ODocument] = {
    val documents: java.util.List[ODocument] = connection.query(new OSQLSynchQuery[ODocument](query))
    documents.toList
  }

  private def getCatalystType(oType: OType): DataType = {
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

private[orientdb] object DefaultOrientDBDocumentWrapper extends OrientDBDocumentWrapper