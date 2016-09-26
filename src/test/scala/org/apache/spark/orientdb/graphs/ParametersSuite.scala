package org.apache.spark.orientdb.graphs

import org.scalatest.{FunSuite, Matchers}

class ParametersSuite extends FunSuite with Matchers {

  test("Minimal valid parameter map is accepted for vertices") {
    val params = Map(
      "dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "vertextype" -> "test_vertex"
    )

    val mergedParams = Parameters.mergeParameters(params)

    mergedParams.dbUrl.get shouldBe params("dburl")
    mergedParams.credentials.get._1 shouldBe params("user")
    mergedParams.credentials.get._2 shouldBe params("password")
    mergedParams.vertexType.get shouldBe params("vertextype")

    Parameters.DEFAULT_PARAMETERS.foreach {
      case (key, value) => mergedParams.parameters(key) shouldBe value
    }
  }

  test("Minimal valid parameter map is accepted for edges") {
    val params = Map(
      "dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "edgetype" -> "test_edge"
    )

    val mergedParams = Parameters.mergeParameters(params)

    mergedParams.dbUrl.get shouldBe params("dburl")
    mergedParams.credentials.get._1 shouldBe params("user")
    mergedParams.credentials.get._2 shouldBe params("password")
    mergedParams.edgeType.get shouldBe params("edgetype")

    Parameters.DEFAULT_PARAMETERS.foreach {
      case (key, value) => mergedParams.parameters(key) shouldBe value
    }
  }

  test("Errors are thrown when mandatory parameters are not provided") {
    def checkMerge(params: Map[String, String]): Unit = {
      intercept[IllegalArgumentException] {
        Parameters.mergeParameters(params)
      }
    }

    val testURL = "remote:127.0.0.1:2424/GratefulDeadConcerts"
    checkMerge(Map("dburl" -> testURL, "user" -> "root", "password" -> "root"))
    checkMerge(Map("user" -> "root", "password" -> "root"))
    checkMerge(Map("dburl" -> testURL, "vertextype" -> "test_vertex"))
    checkMerge(Map("user" -> "root", "password" -> "root", "edgetype" -> "test_edge"))
  }

  test("Must specify either 'vertextype' param, " +
    "or, 'vertextype' and 'query' parameter, " +
    "or 'vertextype' and 'query' parameter and user-defined schema") {
    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root"
      ))
    }

    Parameters.mergeParameters(Map(
      "dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "vertextype" -> "test_vertex"
    ))

    Parameters.mergeParameters(Map(
      "dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "vertextype" -> "test_vertex",
      "query" -> "select * from test_vertex"
    ))
  }

  test("Must specify either 'edgetype' param, " +
    "or, 'edgetype' and 'query' parameter, " +
    "or 'edgetype' and 'query' parameter and user-defined schema") {
    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "user" -> "root",
        "password" -> "root"
      ))
    }

    Parameters.mergeParameters(Map(
      "dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "edgetype" -> "test_edge"
    ))

    Parameters.mergeParameters(Map(
      "dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "edgetype" -> "test_edge",
      "query" -> "select * from test_edge"
    ))
  }

  test("Must specify 'url' parameter and 'user' and 'password' parameters") {
    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts"
      ))
    }

    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "user" -> "root",
        "password" -> "root"
      ))
    }

    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "password" -> "root"
      ))
    }
  }
}