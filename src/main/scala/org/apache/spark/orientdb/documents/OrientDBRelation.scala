package org.apache.spark.orientdb.documents

import com.orientechnologies.orient.core.record.impl.ODocument
import Parameters.MergedParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.slf4j.LoggerFactory

private[orientdb] case class OrientDBRelation(
                                              orientDBWrapper: OrientDBDocumentWrapper,
                                              orientDBClientFactory: OrientDBCredentials => OrientDBClientFactory,
                                              params: MergedParameters,
                                              userSchema: Option[StructType]
                                             ) (@transient val sqlContext: SQLContext)
                  extends BaseRelation
                  with PrunedFilteredScan
                  with InsertableRelation {
  private val log = LoggerFactory.getLogger(getClass)

  // any kind of assertion

  private val tableNameOrSubQuery = params.query.map(q => s"($q)").orElse(params.table.map(_.toString)).get

  override lazy val schema: StructType = {
    userSchema.getOrElse{
      val tableName = params.table.map(_.toString).get
      val conn = orientDBWrapper.getConnection(params)

      try {
        orientDBWrapper.resolveTable(null, tableName)
      } finally {
        conn.close()
      }
    }
  }

  override def toString: String = s"""OrientDBRelation($tableNameOrSubQuery)"""

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val saveMode = if (overwrite) {
      SaveMode.Overwrite
    } else {
      SaveMode.Append
    }
    val writer = new OrientDBWriter(orientDBWrapper, orientDBClientFactory)
    writer.saveToOrientDB(data, saveMode, params)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filterNot(filter => FilterPushdown.buildFilterExpression(schema, filter).isDefined)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    if (requiredColumns.isEmpty) {
      val whereClause = FilterPushdown.buildWhereClause(schema, filters)
      var countQuery = s"select count(*) from $tableNameOrSubQuery $whereClause"

      if (params.query.isDefined) {
        countQuery = tableNameOrSubQuery.drop(1).dropRight(1)
      }

      log.info("count query")
      val connection = orientDBWrapper.getConnection(params)

      try {
        // todo use future
        val results = orientDBWrapper.genericQuery(countQuery)
        if (params.query.isEmpty && results.nonEmpty) {
          val numRows: Long = results.head.field("count")
          val parallelism = sqlContext.getConf("spark.sql.shuffle.partitions", "200").toInt
          val emptyRow = Row.empty
          sqlContext.sparkContext.parallelize(1L to numRows, parallelism).map(_ => emptyRow)
        } else if (params.query.isDefined) {
          val numRows: Long = results.length
          val parallelism = sqlContext.getConf("spark.sql.shuffle.partitions", "200").toInt
          val emptyRow = Row.empty
          sqlContext.sparkContext.parallelize(1L to numRows, parallelism).map(_ => emptyRow)
        } else {
          throw new IllegalStateException("Cannot read count from OrientDB")
        }
      } finally {
        connection.close()
      }
    } else {
      var classname: String = null
      var cluster: String = null
      if (params.query.isEmpty) {
        classname = params.className match {
          case Some(className) => className
          case None =>
            throw new IllegalArgumentException("For save operations you must specify a OrientDB Class " +
              "name with the 'classname' parameter")
        }
        cluster = params.clusterName match {
          case Some(clusterName) => clusterName
          case None =>
            val connection = orientDBWrapper.getConnection(params)
            val schema = connection.getMetadata.getSchema
            val currClass = schema.getClass(classname)
            connection.getClusterNameById(currClass.getDefaultClusterId)
        }
      }

      val filterStr = FilterPushdown.buildWhereClause(schema, filters)
      val connection = orientDBWrapper.getConnection(params)
      var oDocuments: List[ODocument] = List()
      try {
        // todo use Future
        if (params.query.isEmpty) {
          oDocuments = orientDBWrapper.read(cluster, classname, requiredColumns, filterStr)
        } else {
          oDocuments = orientDBWrapper
            .read(null, null, requiredColumns, filterStr, params.query.get)
        }
      } finally {
        connection.close()
      }

      if (params.query.isEmpty) {
        val prunedSchema = pruneSchema(schema, requiredColumns)
        sqlContext.sparkContext.makeRDD(
          oDocuments.map(oDocument => Conversions.convertODocumentsToRows(oDocument, prunedSchema))
        )
      } else {
        assert(oDocuments.nonEmpty)
        val prunedSchema = pruneSchema(schema, oDocuments.head.fieldNames())
        sqlContext.sparkContext.makeRDD(
          oDocuments.map(oDocument => Conversions.convertODocumentsToRows(oDocument, prunedSchema))
        )
      }
    }
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    new StructType(schema.fields.filter(p => columns.contains(p.name)))
  }
}