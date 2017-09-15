package org.apache.spark.orientdb.graphs

import com.tinkerpop.blueprints.{Edge, Vertex}
import org.apache.spark.orientdb.documents.{Conversions, FilterPushdown}
import org.apache.spark.orientdb.graphs.Parameters.MergedParameters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

private[orientdb] case class OrientDBVertexRelation(
                                              orientDBVertexWrapper: OrientDBGraphVertexWrapper,
                                              orientDBClientFactory: OrientDBCredentials => OrientDBClientFactory,
                                              params: MergedParameters,
                                              userSchema: Option[StructType]
                                             ) (@transient val sqlContext: SQLContext)
                  extends BaseRelation
                  with PrunedFilteredScan
                  with InsertableRelation {
  private val log = LoggerFactory.getLogger(getClass)

  private val tableNameOrSubQuery = params.query.map(q => s"($q)").orElse(params.vertexType.map(_.toString)).get

  override lazy val schema: StructType = {
    userSchema.getOrElse{
      val tableName = params.vertexType.map(_.toString).get
      val conn = orientDBVertexWrapper.getConnection(params)

      try {
        orientDBVertexWrapper.resolveTable(tableName)
      } finally {
        orientDBVertexWrapper.close()
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
    val writer = new OrientDBVertexWriter(orientDBVertexWrapper,
      orientDBClientFactory)
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

      try {
        val results = orientDBVertexWrapper.genericQuery(countQuery)
        if (params.query.isEmpty && results.nonEmpty) {
          val numRows: Long = results.head.getProperty[Long]("count")
          val parallelism = sqlContext.getConf("spark.sql.shuffle.partitions", "200").toInt
          val emptyRow = Row.empty
          sqlContext.sparkContext.parallelize(1L to numRows, parallelism).map(_ => emptyRow)
        } else if (params.query.isDefined) {
          val numRows: Long = results.length
          val parallelism = sqlContext.getConf("spark.sql.shuffle.partitions", "200").toInt
          val emptyRow = Row.empty
          sqlContext.sparkContext.parallelize(1L to numRows, parallelism).map(_ => emptyRow)
        } else {
          throw new IllegalArgumentException("Cannot read count for OrientDB graph vertices")
        }
      } finally {
        orientDBVertexWrapper.close()
      }
    } else {
      var vertexTypeName: String = null
      if (params.query.isEmpty) {
        vertexTypeName = params.vertexType match {
          case Some(vertexType) => vertexType
          case None =>
            throw new IllegalArgumentException("For save operations you must specify a OrientDB Graph" +
              " Vertex type name with the 'vertextype' parameter")
        }
      }

      val filterStr = FilterPushdown.buildWhereClause(schema, filters)
      var oVertices: List[Vertex] = List()
      try {
        orientDBVertexWrapper.getConnection(params)
        if (params.query.isEmpty) {
          oVertices = orientDBVertexWrapper.read(vertexTypeName, requiredColumns,
            filterStr, null)
        } else {
          oVertices = orientDBVertexWrapper.read(null, requiredColumns, filterStr,
            params.query.get)
        }
      } finally {
        orientDBVertexWrapper.close()
      }

      if (params.query.isEmpty) {
        val prunedSchema = pruneSchema(schema, requiredColumns)
        sqlContext.sparkContext.makeRDD(
          oVertices.map(vertex => Conversions.convertVerticesToRows(vertex, prunedSchema))
        )
      } else {
        assert(oVertices.nonEmpty)
        val propKeysArray = new Array[String](chooseRecordForSchema(oVertices).getPropertyKeys.size())
        val prunedSchema = pruneSchema(schema, chooseRecordForSchema(oVertices)
          .getPropertyKeys.toArray[String](propKeysArray))
        sqlContext.sparkContext.makeRDD(
          oVertices.map(vertex => Conversions.convertVerticesToRows(vertex, prunedSchema))
        )
      }
    }
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    new StructType(schema.fields.filter(p => columns.contains(p.name)))
  }

  private def chooseRecordForSchema(oVertices: List[Vertex]): Vertex = {
    var maxLen = -1
    var idx: Vertex = null
    oVertices.foreach(oVertex => {
      if (maxLen < oVertex.getPropertyKeys.size()) {
        idx = oVertex
        maxLen = oVertex.getPropertyKeys.size()
      }
    })
    idx
  }
}

private[orientdb] case class OrientDBEdgeRelation(
                                                  orientDBEdgeWrapper: OrientDBGraphEdgeWrapper,
                                                  orientDBClientFactory: OrientDBCredentials => OrientDBClientFactory,
                                                  params: MergedParameters,
                                                  userSchema: Option[StructType]
                                                 ) (@transient val sqlContext: SQLContext)
                  extends BaseRelation
                  with PrunedFilteredScan
                  with InsertableRelation {
  private val log = LoggerFactory.getLogger(getClass)

  private val tableNameOrSubQuery = params.query.map(q => s"($q)").orElse(params.edgeType.map(_.toString)).get

  override lazy val schema: StructType = {
    userSchema.getOrElse{
      val tableName = params.edgeType.map(_.toString).get
      val conn = orientDBEdgeWrapper.getConnection(params)

      try {
        orientDBEdgeWrapper.resolveTable(tableName)
      } finally {
        orientDBEdgeWrapper.close()
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
    val writer = new OrientDBEdgeWriter(orientDBEdgeWrapper, orientDBClientFactory)
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

      try {
        val results = orientDBEdgeWrapper.genericQuery(countQuery)
        if (params.query.isEmpty && results.nonEmpty) {
          val numRows: Long = results.head.getProperty[Long]("count")
          val parallelism = sqlContext.getConf("spark.sql.shuffle.partitions", "200").toInt
          val emptyRow = Row.empty
          sqlContext.sparkContext.parallelize(1L to numRows, parallelism).map(_ => emptyRow)
        } else if (params.query.isDefined) {
          val numRows: Long = results.length
          val parallelism = sqlContext.getConf("spark.sql.shuffle.partitions", "200").toInt
          val emptyRow = Row.empty
          sqlContext.sparkContext.parallelize(1L to numRows, parallelism).map(_ => emptyRow)
        } else {
          throw new IllegalArgumentException("Cannot read count for OrientDB graph edges")
        }
      } finally {
        orientDBEdgeWrapper.close()
      }
    } else {
      var edgeTypeName: String = null
      if (params.query.isEmpty) {
        edgeTypeName = params.edgeType match {
          case Some(edgeType) => edgeType
          case None => throw new IllegalArgumentException("For save operations you must specify a OrientDB Graph" +
            " Edge type name with the 'edgetype' parameter")
        }
      }

      val filterStr = FilterPushdown.buildWhereClause(schema, filters)
      var oEdges: List[Edge] = List()

      try {
        if (params.query.isEmpty) {
          oEdges = orientDBEdgeWrapper.read(edgeTypeName, requiredColumns, filterStr, null)
        } else {
          oEdges = orientDBEdgeWrapper.read(null, requiredColumns, filterStr, params.query.get)
        }
      } finally {
        orientDBEdgeWrapper.close()
      }

      if (params.query.isEmpty) {
        val prunedSchema = pruneSchema(schema, requiredColumns)
        sqlContext.sparkContext.makeRDD(
          oEdges.map(edge => Conversions.convertEdgesToRows(edge, prunedSchema))
        )
      } else {
        assert(oEdges.nonEmpty)
        val propKeysArray = new Array[String](chooseRecordForSchema(oEdges).getPropertyKeys.size())
        val prunedSchema = pruneSchema(schema,
          chooseRecordForSchema(oEdges).getPropertyKeys.toArray[String](propKeysArray))
        sqlContext.sparkContext.makeRDD(
          oEdges.map(edge => Conversions.convertEdgesToRows(edge, prunedSchema))
        )
      }
    }
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    new StructType(schema.fields.filter(p => columns.contains(p.name)))
  }

  private def chooseRecordForSchema(oEdges: List[Edge]): Edge = {
    var maxLen = -1
    var idx: Edge = null
    oEdges.foreach(oEdge => {
      if (maxLen < oEdge.getPropertyKeys.size()) {
        idx = oEdge
        maxLen = oEdge.getPropertyKeys.size()
      }
    })
    idx
  }
}