package org.apache.spark.orientdb.documents

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

private[orientdb] object FilterPushdown {
  /**
    * Build a SQL WHERE clause for the given filters. If a filter cannot be pushed down then no
    * condition will be added to the WHERE clause. If none of the filters can be pushed down then
    * an empty string will be returned.
    *
    * @param schema the schema of the table being queried
    * @param filters an array of filters, the conjunction of which is the filter condition for the
    *                scan.
    */
  def buildWhereClause(schema: StructType, filters: Seq[Filter]): String = {
    val filterExpressions = filters.flatMap(f => buildFilterExpression(schema, f)).mkString(" AND ")
    if (filterExpressions.isEmpty) "" else "WHERE " + filterExpressions
  }

  /**
    * Attempt to convert the given filter into a SQL expression. Returns None if the expression
    * could not be converted.
    */
  def buildFilterExpression(schema: StructType, filter: Filter): Option[String] = {
    def buildComparison(attr: String, value: Any, comparisonOp: String): Option[String] = {
      getTypeForAttribute(schema, attr).map { dataType =>
        val sqlEscapedValue: String = dataType match {
          case StringType => s"\\'${value.toString.replace("'", "\\'\\'")}\\'"
          case DateType => s"\\'${value.asInstanceOf[Date]}\\'"
          case TimestampType => s"\\'${value.asInstanceOf[Timestamp]}\\'"
          case _ => value.toString
        }
        s"""$attr $comparisonOp $sqlEscapedValue"""
      }
    }

    filter match {
      case EqualTo(attr, value) => buildComparison(attr, value, "=")
      case LessThan(attr, value) => buildComparison(attr, value, "<")
      case GreaterThan(attr, value) => buildComparison(attr, value, ">")
      case LessThanOrEqual(attr, value) => buildComparison(attr, value, "<=")
      case GreaterThanOrEqual(attr, value) => buildComparison(attr, value, ">=")
      case IsNotNull(attr) =>
        getTypeForAttribute(schema, attr).map(dataType => s"""$attr IS NOT NULL""")
      case IsNull(attr) =>
        getTypeForAttribute(schema, attr).map(dataType => s"""$attr IS NULL""")
      case _ => None
    }
  }

  /**
    * Use the given schema to look up the attribute's data type. Returns None if the attribute could
    * not be resolved.
    */
  private def getTypeForAttribute(schema: StructType, attribute: String): Option[DataType] = {
    if (schema.fieldNames.contains(attribute)) {
      Some(schema(attribute).dataType)
    } else {
      None
    }
  }
}