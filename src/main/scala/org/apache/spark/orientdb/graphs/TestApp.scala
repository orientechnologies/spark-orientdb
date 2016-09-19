package org.apache.spark.orientdb.graphs

import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object TestApp extends App {
  val graphFactory = new OrientGraphFactory("remote:127.0.0.1:2424/GratefulDeadConcerts",
                                          "root", "root")

  val graph = graphFactory.getNoTx

  val vertexType = graph.createVertexType("v124")
  vertexType.createProperty("id1", OType.INTEGER)
  vertexType.createProperty("name", OType.STRING)

  val inVertex = graph.addVertex("v124", null)
  val outVertex = graph.addVertex("v124", null)

  val edgeType = graph.createEdgeType("e124")
  edgeType.createProperty("id1", OType.INTEGER)
  edgeType.createProperty("name", OType.STRING)

  val edge = graph.addEdge(null, inVertex, outVertex, "e124")
  edge.setProperty("id1", 1)
  edge.setProperty("name", "Subhobrata")

  val query = graph.command(new OCommandSQL("select id1,name from e124"))
    .execute[java.lang.Iterable[Edge]]()

  val iterator = query.iterator()

  while (iterator.hasNext) {
    val data = iterator.next()
    println()
  }
}

object TestApp1 extends App {
  val credentials = new OrientDBCredentials {
    dbUrl = "remote:127.0.0.1:2424/GratefulDeadConcerts"
    username = "root"
    password = "root"
  }
  val wrapper = new OrientDBGraphVertexWrapper()
  val writer = new OrientDBVertexWriter(wrapper,
    credentials => new OrientDBClientFactory(credentials))

  val conf = new SparkConf().setAppName("TestApp1").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
    StructType(StructField("a", IntegerType) :: Nil))

  val parameters = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts", "user" -> "root",
                    "password" -> "root", "vertexType" -> "vclass10")

  writer.saveToOrientDB(df, SaveMode.Overwrite, Parameters.mergeParameters(parameters))
}

object TestApp2 extends App {
  val credentials = new OrientDBCredentials {
    dbUrl = "remote:127.0.0.1:2424/GratefulDeadConcerts"
    username = "root"
    password = "root"
  }
  val wrapper = new OrientDBGraphEdgeWrapper()
  val writer = new OrientDBEdgeWriter(wrapper,
    credentials => new OrientDBClientFactory(credentials))

  val conf = new SparkConf().setAppName("TestApp2").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row("vedge1", "vedge2", 1))),
    StructType(Seq(StructField("inVertex", StringType), StructField("outVertex", StringType),
      StructField("a", IntegerType))))

  val parameters = Map("dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts", "user" -> "root",
    "password" -> "root", "edgeType" -> "eclass28")
  writer.saveToOrientDB(df, SaveMode.Overwrite, Parameters.mergeParameters(parameters))
}