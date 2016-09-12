package org.apache.spark.orientdb.documents

import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.slf4j.LoggerFactory

class DefaultSource(orientDBWrapper: OrientDBDocumentWrapper,
                    orientDBClientFactory: OrientDBCredentials => OrientDBClientFactory)
      extends RelationProvider
      with SchemaRelationProvider
      with CreatableRelationProvider {

  private val log = LoggerFactory.getLogger(getClass)

  def this() = this(DefaultOrientDBDocumentWrapper, orientDBCredentials => new OrientDBClientFactory(orientDBCredentials))

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val params = Parameters.mergeParameters(parameters)

    if (params.query.isDefined && params.className.isEmpty) {
      throw new IllegalArgumentException("Along with the 'query' parameter you must specify either 'class' parameter or"+
      " user-defined schema")
    }

    OrientDBRelation(orientDBWrapper, orientDBClientFactory, params, None)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    val params = Parameters.mergeParameters(parameters)
    OrientDBRelation(orientDBWrapper, orientDBClientFactory, params, Some(schema))(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val params = Parameters.mergeParameters(parameters)
    val classname = params.className.getOrElse{
      throw new IllegalArgumentException("For save operations you must specify a OrientDB Class " +
        "name with the 'classname' parameter")
    }

    val cluster = params.clusterName.getOrElse{
      log.warn("Orient DB cluster name not specified. Using default Cluster Id for the class specified")
    }

    def tableExists: Boolean = {
      val connection = orientDBWrapper.getConnection(params)
      try {
        orientDBWrapper.doesClassExists(classname)
      } finally {
        connection.close()
      }
    }

    val (doSave, dropExisting) = mode match {
      case SaveMode.Append => (true, false)
      case SaveMode.Overwrite => (true, true)
      case SaveMode.ErrorIfExists =>
        if (tableExists) {
          sys.error(s"Class $classname already exists! (SaveMode is set to ErrorIfExists)")
        } else {
          (true, false)
        }
      case SaveMode.Ignore =>
        if (tableExists) {
          log.info(s"Class $classname already exists. Ignoring save request.")
          (false, false)
        } else {
          (true, false)
        }
    }

    if (doSave) {
      val updatedParams = parameters.updated("overwrite", dropExisting.toString)
      new OrientDBWriter(orientDBWrapper, orientDBClientFactory)
        .saveToOrientDB(data, mode, Parameters.mergeParameters(updatedParams))
    }
    createRelation(sqlContext, parameters)
  }
}