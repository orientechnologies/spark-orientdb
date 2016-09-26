package org.apache.spark.orientdb.graphs

import java.util

import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.tinkerpop.blueprints.impls.orient.{OrientBaseGraph, OrientEdge}

class MockEdge(graph: OrientBaseGraph, record: OIdentifiable)
  extends OrientEdge(graph, record) {

  override def setProperty(key: String, value: Object): Unit = {
    this.getRecord().field(key, value)
  }

  override def getPropertyKeys: util.Set[String] = {
    val fieldNames = this.getRecord().fieldNames()
    val length = fieldNames.length

    var count = 0
    val result = new util.HashSet[String]()
    while (count < length) {
      result.add(fieldNames(count))
      count = count + 1
    }
    result
  }
}