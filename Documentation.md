Apache Spark datasource for OrientDB
====================================

## Introduction

This documentation discusses the topic of how to connect Apache Spark to OrientDB. [Apache Spark](http://spark.apache.org/) is the widely popular engine for large-scale data processing.
Here, we will discuss how to use the [**Apache Spark datasource for OrientDB**](https://github.com/sbcd90/spark-orientdb) to leverage Spark's capabilities while using OrientDB as the datastore. 

## Installation Guide

To use the Apache Spark datasource for OrientDB inside a Spark application, the following steps need to be performed.
 
- Add the repository location to `pom.xml`. 

```
<repository>
   <id>bintray</id>
   <name>bintray</name>
   <url>https://dl.bintray.com/sbcd90/org.apache.spark/</url>
</repository>
``` 

- Add the datasource as a maven dependency in `pom.xml`.

```
<dependency>
   <groupId>org.apache.spark</groupId>
   <artifactId>spark-orientdb-{spark.version}_2.10</artifactId>
   <version>1.3</version>
</dependency>
```


## Configuration

The datasource is supported for Apache Spark version 1.6+ & OrientDB 2.2.0+. 

## API Reference

The **Apache Spark datasource for OrientDB** allows users to leverage the Apache Spark datasource api s for reading and writing data from OrientDB. The datasource loads a collection of OrientDB documents into a dataframe & an OrientDB graph containing a set of vertices & edges into a Graphframe. 

The complete api reference for using the **Apache Spark datasource for OrientDB** is provided below.

### OrientDB Documents

#### Write api:

```
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
sqlContext.createDataFrame(sc.parallelize(Array(1, 2, 3, 4, 5)), 
      StructType(Seq(StructField("id", IntegerType)))
      .write
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER).option("password", ORIENTDB_PASSWORD)
      .option("class", test_class)
      .mode(SaveMode.Overwrite)
      .save()
```

#### Read api:

```
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("class", test_class)
      .option("query", s"select * from $test_table where teststring = 'asdf'")
      .load()
```

#### Query using OrientDB SQL:

```
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val loadedDf = sqlContext.read
      .format("org.apache.spark.orientdb.documents")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("class", test_class)
      .option("query", s"select * from $test_table where teststring = 'asdf'")
      .load()
```

### OrientDB Graphs:

#### Create Vertex api:

```
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
sqlContext.createDataFrame(sc.parallelize(Array(1, 2, 3, 4, 5)),
      StructType(Seq(StructField("id", IntegerType)))
      .write
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type2)
      .mode(SaveMode.Overwrite)
      .save()
```

#### Create Edge api:

```
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
sqlContext.createDataFrame(
      sc.parallelize(Seq(
            Row(1, 2, "friends"),
            Row(2, 3, "enemy"),
            Row(3, 4, "friends"),
            Row(4, 1, "enemy")
      )),
      StructType(Seq(
            StructField("src", IntegerType),
            StructField("dst", IntegerType),
            StructField("relationship", StringType)
          )))
      .write
      .format("org.apache.spark.orientdb.graphs")
      .option("dburl", ORIENTDB_CONNECTION_URL)
      .option("user", ORIENTDB_USER)
      .option("password", ORIENTDB_PASSWORD)
      .option("vertextype", test_vertex_type2)
      .option("edgetype", test_edge_type2)
      .mode(SaveMode.Overwrite)
      .save()
```

#### Read Vertex api:

```
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val loadedDf = sqlContext.read
                    .format("org.apache.spark.orientdb.graphs")
                    .option("dburl", ORIENTDB_CONNECTION_URL)
                    .option("user", ORIENTDB_USER)
                    .option("password", ORIENTDB_PASSWORD)
                    .option("vertextype", test_vertex_type2)
                    .load()
```

#### Read edge api:

```
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val loadedDf = sqlContext.read
                   .format("org.apache.spark.orientdb.graphs")
                   .option("dburl", ORIENTDB_CONNECTION_URL)
                   .option("user", ORIENTDB_USER)
                   .option("password", ORIENTDB_PASSWORD)
                   .option("edgetype", test_edge_type2)
                   .load()
```

#### Query using OrientDB Graph SQL:

```
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val loadedVerticesDf = sqlContext.read
                 .format("org.apache.spark.orientdb.graphs")
                 .option("dburl", ORIENTDB_CONNECTION_URL)
                 .option("user", ORIENTDB_USER)
                 .option("password", ORIENTDB_PASSWORD)
                 .option("vertextype", test_vertex_type2)
                 .option("query", s"select * from $test_vertex_type2 where teststring = 'asdf'")
                 .load()
                 
val loadedEdgesDf = sqlContext.read
                 .format("org.apache.spark.orientdb.graphs")
                 .option("dburl", ORIENTDB_CONNECTION_URL)
                 .option("user", ORIENTDB_USER)
                 .option("password", ORIENTDB_PASSWORD)
                 .option("edgetype", test_edge_type2)
                 .option("query", s"select * from $test_edge_type2 where relationship = 'friends'")
                 .load()                 
```

### Integration with GraphFrames

```
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val loadedVerticesDf = sqlContext.read
                 .format("org.apache.spark.orientdb.graphs")
                 .option("dburl", ORIENTDB_CONNECTION_URL)
                 .option("user", ORIENTDB_USER)
                 .option("password", ORIENTDB_PASSWORD)
                 .option("vertextype", test_vertex_type2)
                 .option("query", s"select * from $test_vertex_type2 where teststring = 'asdf'")
                 .load()
                 
val loadedEdgesDf = sqlContext.read
                 .format("org.apache.spark.orientdb.graphs")
                 .option("dburl", ORIENTDB_CONNECTION_URL)
                 .option("user", ORIENTDB_USER)
                 .option("password", ORIENTDB_PASSWORD)
                 .option("edgetype", test_edge_type2)
                 .option("query", s"select * from $test_edge_type2 where relationship = 'friends'")
                 .load()
                 
val g = GraphFrame(loadedVerticesDf, loadedEdgesDf)                 
```

## Examples

### OrientDB Documents

```
package org.apache.spark.orientdb.documents

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameTest extends App {
  val conf = new SparkConf().setAppName("DataFrameTest").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._
  val df = sc.parallelize(Array(1, 2, 3, 4, 5)).toDF("id")

  df.write.format("org.apache.spark.orientdb.documents")
    .option("dburl", "<db url>")
    .option("user", "****")
    .option("password", "****")
    .option("class", "test_class")
    .mode(SaveMode.Overwrite)
    .save()

  val resultDf = sqlContext.read
    .format("org.apache.spark.orientdb.documents")
    .option("dburl", "<db url>")
    .option("user", "****")
    .option("password", "****")
    .option("class", "test_class")
    .load()

  resultDf.show()
}
```

### OrientDB Graph

```
package org.apache.spark.orientdb.graphs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.graphframes.GraphFrame

object GraphFrameTest extends App {
  val conf = new SparkConf().setAppName("MainApplication").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._
  val df = sc.parallelize(Array(1, 2, 3, 4, 5)).toDF("id")

  df.write.format("org.apache.spark.orientdb.graphs")
    .option("dburl", "<db url>")
    .option("user", "****")
    .option("password", "****")
    .option("vertextype", "v104")
    .mode(SaveMode.Overwrite)
    .save()

  val vertices = sqlContext.read
    .format("org.apache.spark.orientdb.graphs")
    .option("dburl", "<db url>")
    .option("user", "****")
    .option("password", "****")
    .option("vertextype", "v104")
    .load()

  var inVertex: Integer = null
  var outVertex: Integer = null
  vertices.collect().foreach(row => {
    if (inVertex == null) {
      inVertex = row.getAs[Integer]("id")
    }
    if (outVertex == null) {
      outVertex = row.getAs[Integer]("id")
    }
  })

  val df1 = sqlContext.createDataFrame(sc.parallelize(Seq(Row("friends", "1", "2"),
    Row("enemies", "2", "3"), Row("friends", "3", "1"))),
    StructType(List(StructField("relationship", StringType), StructField("src", StringType),
      StructField("dst", StringType))))

  df1.write.format("org.apache.spark.orientdb.graphs")
    .option("dburl", "<db url>")
        .option("user", "****")
        .option("password", "****")
    .option("vertextype", "v104")
    .option("edgetype", "e104")
    .mode(SaveMode.Overwrite)
    .save()

  val edges = sqlContext.read
    .format("org.apache.spark.orientdb.graphs")
    .option("dburl", "<db url>")
    .option("user", "****")
    .option("password", "****")
    .option("edgetype", "e104")
    .load()

  edges.show()

  val g = GraphFrame(vertices, edges)
  g.inDegrees.show()
  println(g.edges.filter("relationship = 'friends'").count())
}
```