package org.apache.spark.orientdb.graphs

private[orientdb] object Parameters {
  val DEFAULT_PARAMETERS: Map[String, String] = Map(
    "overwrite" -> "false"
  )

  def mergeParameters(userParameters: Map[String, String]): MergedParameters = {
    if (!userParameters.contains("dburl")) {
      throw new IllegalArgumentException("A Orient DB URL must be provided with 'dburl' parameter")
    }

    if (!userParameters.contains("vertextype") && !userParameters.contains("edgetype") && !userParameters.contains("query")) {
      throw new IllegalArgumentException("You must specify one of Orient DB Vertex type in the 'vertextype'" +
        " parameter or Orient DB Edge type in the 'edgetype' parameter or a user specified query using 'query' parameter")
    }

    if (!userParameters.contains("user") ||  !userParameters.contains("password")) {
      throw new IllegalArgumentException("You must specify both the OrientDB username in 'user' parameter &" +
        " OrientDB password in the 'password' parameter")
    }
    MergedParameters(DEFAULT_PARAMETERS ++ userParameters)
  }

  case class MergedParameters(parameters: Map[String, String]) {

    /**
      * The Orient DB Graph vertex Type to be used to load & write data
      */
    def vertexType: Option[String] = parameters.get("vertextype").orElse(None)

    /**
      * The Orient DB Graph edge Type to be used to load & write data
      */
    def edgeType: Option[String] = parameters.get("edgetype").orElse(None)

    def createVertexIfNotExist: Boolean = parameters.get("createVertexIfNotExist").isDefined &&
                                          parameters("createVertexIfNotExist") == "true"

    def lightWeightEdge: Boolean = parameters.get("lightWeightEdge").isDefined &&
                                   parameters("lightWeightEdge") == "true"

    /**
      * The Orient DB Graph sql query to be used for loading data
      */
    def query: Option[String] = parameters.get("query").orElse(None)

    /**
      * Username & Password for authentication with OrientDB
      */
    def credentials: Option[(String, String)] = {
      for {
        username <- parameters.get("user")
        password <- parameters.get("password")
      } yield (username, password)
    }

    /**
      * A url in the format
      * remote:<hostname>:<port>/<database>
      */
    def dbUrl: Option[String] = parameters.get("dburl")

    /**
      * cluster name in Orient DB
      */
    def cluster: Option[String] = parameters.get("cluster").orElse(None)

    /**
      * mention linked properties in the form "vertextype/edgetype" - "linkedType-linked vertextype/edgetype"
      */
    def linkedType: Option[Map[String, String]] =
      if (parameters.exists(paramPair => paramPair._2.contains("linkedType")))
        Some(parameters.filter(paramPair => paramPair._2.contains("linkedType")))
      else None
  }
}
