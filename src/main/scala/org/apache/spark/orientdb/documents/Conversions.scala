package org.apache.spark.orientdb.documents

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
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

  def convertRowsToODocuments(row: Row): ODocument = {
    val oDocument = new ODocument()
    row.schema.fields.foreach(field => {
      oDocument.field(field.name, row.getAs[field.dataType.type](field.name),
        Conversions.sparkDTtoOrientDBDT(field.dataType))
    })
    oDocument
  }

  def orientDBDTtoSparkDT(dataType: DataType, field: String) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dataType match {
      case ByteType => field.toByte
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
      case TimestampType => Timestamp.valueOf(field)
      case other => throw new UnsupportedOperationException(s"Unexpected DataType $dataType")
    }
  }

  def convertODocumentsToRows(oDocument: ODocument, schema: StructType): Row = {
    val converted: scala.collection.mutable.IndexedSeq[Any] = mutable.IndexedSeq.fill(schema.length)(null)
    val fields = oDocument.fieldValues()
    var i = 0
    while (i < schema.length) {
      val data = fields(i)
      converted(i) = if (data == null) null else orientDBDTtoSparkDT(schema.fields(i).dataType,
        fields(i).toString)
      i = i + 1
    }
    Row.fromSeq(converted)
  }
}