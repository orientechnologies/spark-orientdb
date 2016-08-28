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
      val countQuery = s"select count(*) from $tableNameOrSubQuery $whereClause"
      log.info("count query")
      val connection = orientDBWrapper.getConnection(params)

      try {
        // todo use future
        val results = orientDBWrapper.genericQuery(countQuery)
        if (results.nonEmpty) {
          val numRows: Long = results.head.field("count")
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
      val classname = params.className match {
        case Some(className) => className
        case None => throw new IllegalArgumentException("For save operations you must specify a OrientDB Class " +
          "name with the 'classname' parameter")
      }
      val cluster = params.clusterName match {
        case Some(clusterName) => clusterName
        case None =>
          val connection = orientDBWrapper.getConnection(params)
          val schema = connection.getMetadata.getSchema
          val currClass = schema.getClass(classname)
          connection.getClusterNameById(currClass.getDefaultClusterId)
      }

      val filterStr = FilterPushdown.buildWhereClause(schema, filters)
      val connection = orientDBWrapper.getConnection(params)
      var oDocuments: List[ODocument] = List()
      try {
        // todo use Future
        oDocuments = orientDBWrapper.read(cluster, classname, requiredColumns, filterStr)
      } finally {
        connection.close()
      }

      val prunedSchema = pruneSchema(schema, requiredColumns)
      sqlContext.sparkContext.makeRDD(
        oDocuments.map(oDocument => Conversions.convertODocumentsToRows(oDocument, prunedSchema))
      )
    }
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }
}