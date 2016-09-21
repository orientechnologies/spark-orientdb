package org.apache.spark.orientdb.documents

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import com.tinkerpop.blueprints.{Direction, Edge, Vertex}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable

private[orientdb] object Conversions {
  def sparkDTtoOrientDBDT(dataType: DataType): OType = {
    dataType match {
      case ByteType => OType.BYTE
      case ShortType => OType.SHORT
      case IntegerType => OType.INTEGER
      case LongType => OType.LONG
      case FloatType => OType.FLOAT
      case DoubleType => OType.DOUBLE
      case _: DecimalType => OType.DECIMAL
      case StringType => OType.STRING
      case BinaryType => OType.BINARY
      case BooleanType => OType.BOOLEAN
      case DateType => OType.DATE
      case TimestampType => OType.DATETIME
      case other => throw new UnsupportedOperationException(s"Unexpected DataType $dataType")
    }
  }

/*  def orientDBDTtoSparkDT(dataType: OType): DataType = {
    dataType match {
      case OType.BYTE => ByteType
      case OType.SHORT => ShortType
      case OType.INTEGER => IntegerType
      case OType.LONG => LongType
      case OType.FLOAT => FloatType
      case OType.DOUBLE => DoubleType
      case OType.DECIMAL =>
        DecimalType(DecimalType.MAX_PRECISION, DecimalType.MAX_SCALE)
      case OType.STRING => StringType
      case OType.BINARY => BinaryType
      case OType.BOOLEAN => BooleanType
      case OType.DATE => DateType
      case OType.DATETIME => TimestampType
      case other => throw new UnsupportedOperationException(s"Unexpected DataType $dataType")
    }
  } */

  def convertRowsToODocuments(row: Row): ODocument = {
    val oDocument = new ODocument()
    row.schema.fields.foreach(field => {
      oDocument.field(field.name, row.getAs[field.dataType.type](field.name),
        Conversions.sparkDTtoOrientDBDT(field.dataType))
    })
    oDocument
  }

  def orientDBDTtoSparkDT(dataType: DataType, field: String) = {
    if (field == null)
      field
    else {
      val dateFormat = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy")
      dataType match {
        case ByteType => java.lang.Byte.valueOf(field)
        case ShortType => field.toShort
        case IntegerType => field.toInt
        case LongType => field.toLong
        case FloatType => field.toFloat
        case DoubleType => field.toDouble
        case _: DecimalType => field.asInstanceOf[java.math.BigDecimal]
        case StringType => field
        case BinaryType => field
        case BooleanType => field.toBoolean
        case DateType => new Date(dateFormat.parse(field).getTime)
        case TimestampType => new Timestamp(dateFormat.parse(field).getTime)
        case other => throw new UnsupportedOperationException(s"Unexpected DataType $dataType")
      }
    }
  }

/*  def orientDBDTtoSparkDT(dataType: OType, field: String) = {
    if (field == null)
      field
    else {
      val dateFormat = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy")
      dataType match {
        case OType.BYTE => field.toByte
        case OType.SHORT => field.toShort
        case OType.INTEGER => field.toInt
        case OType.LONG => field.toLong
        case OType.FLOAT => field.toFloat
        case OType.DOUBLE => field.toDouble
        case OType.DECIMAL => field.asInstanceOf[java.math.BigDecimal]
        case OType.STRING => field
        case OType.BINARY => field
        case OType.BOOLEAN => field.toBoolean
        case OType.DATE => new Date(dateFormat.parse(field).getTime)
        case OType.DATETIME => new Timestamp(dateFormat.parse(field).getTime)
        case _ => throw new UnsupportedOperationException(s"Unexpected DataType $dataType")
      }
    }
  } */

  def convertODocumentsToRows(oDocument: ODocument, schema: StructType): Row = {
    val converted: scala.collection.mutable.IndexedSeq[Any] = mutable.IndexedSeq.fill(schema.length)(null)
    val fieldNames = oDocument.fieldNames()
    val fieldValues = oDocument.fieldValues()

    var i = 0
    while (i < schema.length) {
      if (fieldNames.contains(schema.fields(i).name)) {
        val idx = fieldNames.indexOf(schema.fields(i).name)
        val value = fieldValues(idx)

        converted(i) = orientDBDTtoSparkDT(schema.fields(i).dataType, value.toString)
      } else {
        converted(i) = null
      }
      i = i + 1
    }

    Row.fromSeq(converted)
  }

  def convertVerticesToRows(vertex: Vertex, schema: StructType): Row = {
    val converted: scala.collection.mutable.IndexedSeq[Any] = mutable.IndexedSeq.fill(schema.length)(null)
    val fieldNames = vertex.getPropertyKeys

    var i = 0
    while (i < schema.length) {
      if (fieldNames.contains(schema.fields(i).name)) {
        val value = vertex.getProperty[Object](schema.fields(i).name)

        converted(i) = orientDBDTtoSparkDT(schema.fields(i).dataType, value.toString)
      } else {
        converted(i) = null
      }
      i = i + 1
    }

    Row.fromSeq(converted)
  }

  def convertEdgesToRows(edge: Edge, schema: StructType): Row = {
    val converted: scala.collection.mutable.IndexedSeq[Any] = mutable.IndexedSeq.fill(schema.length)(null)
    val fieldNames = edge.getPropertyKeys

    var i = 0
    while (i < schema.length) {
      if (fieldNames.contains(schema.fields(i).name)) {
        val value = edge.getProperty[Object](schema.fields(i).name)

        converted(i) = orientDBDTtoSparkDT(schema.fields(i).dataType, value.toString)
      } else {
        converted(i) = null
      }
      i = i + 1
    }

    Row.fromSeq(converted)
  }
}