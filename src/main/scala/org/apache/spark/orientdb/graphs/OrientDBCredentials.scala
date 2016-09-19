package org.apache.spark.orientdb.graphs

trait OrientDBCredentials extends Serializable {
  var dbUrl: String = null
  var username: String = null
  var password: String = null
}