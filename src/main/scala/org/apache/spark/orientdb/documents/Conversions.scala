package org.apache.spark.orientdb.documents

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Map
import java.util.function.Consumer

import com.orientechnologies.orient.core.db.record._
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.record.impl.ODocument
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import com.tinkerpop.blueprints.{Edge, Vertex}
import org.apache.spark.orientdb.udts._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
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
      case _: EmbeddedListType => OType.EMBEDDEDLIST
      case _: EmbeddedSetType => OType.EMBEDDEDSET
      case _: EmbeddedMapType => OType.EMBEDDEDMAP
      case _: LinkListType => OType.LINKLIST
      case _: LinkSetType => OType.LINKSET
      case _: LinkMapType => OType.LINKMAP
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
      oDocument.field(field.name, getField(row, field),
        Conversions.sparkDTtoOrientDBDT(field.dataType))
    })
    oDocument
  }

  def orientDBDTtoSparkDT(dataType: DataType, field: AnyRef) = {
    if (field == null)
      field
    else {
      val dateFormat = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy")
      dataType match {
        case ByteType => java.lang.Byte.valueOf(field.toString)
        case ShortType => field.toString.toShort
        case IntegerType => field.toString.toInt
        case LongType => field.toString.toLong
        case FloatType => field.toString.toFloat
        case DoubleType => field.toString.toDouble
        case _: DecimalType => field.asInstanceOf[java.math.BigDecimal]
        case StringType => field
        case BinaryType => field
        case BooleanType => field.toString.toBoolean
        case DateType => new Date(dateFormat.parse(field.toString).getTime)
        case TimestampType => new Timestamp(dateFormat.parse(field.toString).getTime)
        case _: EmbeddedListType =>
          var elements = Array[Any]()
          field.asInstanceOf[OTrackedList[Any]].forEach(new Consumer[Any] {
            override def accept(t: Any): Unit = elements :+= t
          })
          new EmbeddedList(elements)
        case _: EmbeddedSetType =>
          var elements = Array[Any]()
          field.asInstanceOf[OTrackedSet[Any]].forEach(new Consumer[Any] {
            override def accept(t: Any): Unit = elements :+= t
          })
          new EmbeddedSet(elements)
        case _: EmbeddedMapType =>
          var elements = mutable.Map[Any, Any]()
          field.asInstanceOf[OTrackedMap[Any]].entrySet().forEach(new Consumer[Map.Entry[AnyRef, Any]] {
            override def accept(t: Map.Entry[AnyRef, Any]): Unit = {
              elements.put(t.getKey, t.getValue)
            }
          })
          new EmbeddedMap(elements.toMap)
        case _: LinkListType =>
          var elements = Array[ORecord]()
          field.asInstanceOf[ORecordLazyList].forEach(new Consumer[OIdentifiable] {
            override def accept(t: OIdentifiable): Unit = t match {
              case recordId: ORecordId =>
                elements:+= new ODocument(recordId).asInstanceOf[ORecord]
              case record: ORecord =>
                elements :+= record
            }
          })
          new LinkList(elements)
        case _: LinkSetType =>
          var elements = Array[ORecord]()
          field.asInstanceOf[ORecordLazySet].forEach(new Consumer[OIdentifiable] {
            override def accept(t: OIdentifiable): Unit = t match {
              case recordId: ORecordId =>
                elements:+= new ODocument(recordId).asInstanceOf[ORecord]
              case record: ORecord =>
                elements :+= record
            }
          })
          new LinkSet(elements)
        case _: LinkMapType =>
          val elements = mutable.Map[String, ORecord]()
          field.asInstanceOf[ORecordLazyMap].entrySet().forEach(new Consumer[Map.Entry[AnyRef, OIdentifiable]] {
            override def accept(t: Map.Entry[AnyRef, OIdentifiable]): Unit = {
              elements.put(t.getKey.toString, t.getValue match {
                case recordId: ORecordId =>
                  new ODocument(recordId).asInstanceOf[ORecord]
                case record: ORecord =>
                  record
              })
            }
          })
          new LinkMap(elements.toMap)
        case other => throw new UnsupportedOperationException(s"Unexpected DataType $dataType")
      }
    }
  }

  def convertRowToGraph(row: Row, count: Int): AnyRef = {
    getField(row, row.schema.fields(count))
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

        converted(i) = orientDBDTtoSparkDT(schema.fields(i).dataType, value)
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

        converted(i) = orientDBDTtoSparkDT(schema.fields(i).dataType, value)
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

        converted(i) = orientDBDTtoSparkDT(schema.fields(i).dataType, value)
      } else {
        converted(i) = null
      }
      i = i + 1
    }

    Row.fromSeq(converted)
  }

  private def getField(row: Row, field: StructField) = field.dataType.typeName match {
    case "embeddedlist" => row.getAs[field.dataType.type](field.name).asInstanceOf[EmbeddedList].elements
    case "embeddedset" => row.getAs[field.dataType.type](field.name).asInstanceOf[EmbeddedSet].elements
    case "embeddedmap" => mapAsJavaMap(row.getAs[field.dataType.type](field.name).asInstanceOf[EmbeddedMap].elements)
    case "linklist" => row.getAs[field.dataType.type](field.name).asInstanceOf[LinkList].elements
    case "linkset" => row.getAs[field.dataType.type](field.name).asInstanceOf[LinkSet].elements
    case "linkmap" => mapAsJavaMap(row.getAs[field.dataType.type](field.name).asInstanceOf[LinkMap].elements)
    case _ => row.getAs[field.dataType.type](field.name)
  }
}