package org.apache.spark.orientdb.documents

private[orientdb] object Parameters {
  val DEFAULT_PARAMETERS: Map[String, String] = Map(
    "overwrite" -> "false"
  )

  def mergeParameters(userParameters: Map[String, String]): MergedParameters = {
    if (!userParameters.contains("dburl")) {
      throw new IllegalArgumentException("A Orient DB URL must be provided with 'dburl' parameter")
    }

    if (!userParameters.contains("class") && !userParameters.contains("query")) {
      throw new IllegalArgumentException("You must specify Orient DB class name with 'class' " +
        "parameter or a query with the 'query' parameter. If it is a 'query' you must define your own" +
        " schema or specify orientdb 'class' name.")
    }

    if (!userParameters.contains("user") || !userParameters.contains("password")) {
      throw new IllegalArgumentException("A Orient DB username & password must be provided" +
        " with 'user' & 'password' parameters respectively")
    }
    MergedParameters(DEFAULT_PARAMETERS ++ userParameters)
  }

  case class MergedParameters(parameters: Map[String, String]) {

    /**
      * OrientDB Table from where to load and write data.
      */
    def table: Option[TableName] = parameters.get("class").map(_.trim).flatMap(dbTable => {
      /**
        * this case is going to be handled in query variable.
        */
      if (dbTable.startsWith("(") && dbTable.endsWith(")"))
        None
      else
        Some(TableName(dbTable))
    })

    /**
      * The OrientDB query to be used when loading data
      */
    def query: Option[String] = parameters.get("query").orElse({
      parameters.get("class")
        .map(_.trim)
        .filter(t => t.startsWith("(") && t.endsWith(")"))
        .map(t => t.drop(1).dropRight(1))
    })

    /**
      * Username & Password for authenticating with OrientDB.
      */
    def credentials: Option[(String, String)] = {
      for {
        username <- parameters.get("user")
        password <- parameters.get("password")
      } yield (username, password)
    }

    /**
      * A url in the format
      *
      * remote:<hostname>:<port>/<database>
      */
    def dbUrl: Option[String] = parameters.get("dburl")

    /**
      * class name in Orient DB
      */
    def className: Option[String] = parameters.get("class")

    /**
      * cluster name in Orient DB
      */
    def clusterName: Option[String] = parameters.get("cluster")
  }
}