package org.apache.spark.orientdb.documents

private[orientdb] case class TableName(var unescapedTableName: String) {
  /**
    * drop the quotes from the two ends
    */
  if (unescapedTableName.startsWith("\"") && unescapedTableName.endsWith("\""))
    unescapedTableName = unescapedTableName.drop(1).dropRight(1)

  private def quote(str: String) = '"' + str.replace("\"", "\"\"") + '"'
  def escapedTableName: String = unescapedTableName

  override def toString: String = s"$escapedTableName"
}