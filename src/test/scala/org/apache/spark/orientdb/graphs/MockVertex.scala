package org.apache.spark.orientdb.graphs

import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.tinkerpop.blueprints.impls.orient.{OrientBaseGraph, OrientVertex}

class MockVertex(graph: OrientBaseGraph, record: OIdentifiable)
  extends OrientVertex(graph, record) {
  override def setProperty(key: String, value: Object): Unit = {
    this.getRecord().field(key, value)
  }
}