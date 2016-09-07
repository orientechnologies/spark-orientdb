package org.apache.spark.orientdb.documents

import org.scalatest.{FunSuite, Matchers}

class ParametersSuite extends FunSuite with Matchers {

  test("Minimal valid parameter map is accepted") {
    val params = Map(
      "dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "class" -> "test_class"
    )

    val mergedParams = Parameters.mergeParameters(params)

    mergedParams.dbUrl shouldBe params.get("dburl")
    mergedParams.credentials.get._1 shouldBe params.get("user").get
    mergedParams.credentials.get._2 shouldBe params.get("password").get
    mergedParams.className.get shouldBe params.get("class").get

    Parameters.DEFAULT_PARAMETERS foreach {
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
    checkMerge(Map("dburl" -> testURL, "class" -> "test_class"))
    checkMerge(Map("dburl" -> testURL, "user" -> "root", "password" -> "root"))
    checkMerge(Map("user" -> "root", "password" -> "root"))
  }

  test("Must specify either 'class' param, or, 'class' and 'query' parameter, or 'query' parameter and " +
    "user-defined schema") {
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
      "class" -> "test_class"
    ))

    Parameters.mergeParameters(Map(
      "dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "class" -> "test_class",
      "query" -> "select * from test_class"
    ))
  }

  test("Must specify 'url' parameter and 'user' and 'password' parameters") {
    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
        "class" -> "test_class"
      ))
    }

    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "user" -> "root",
        "password" -> "root",
        "class" -> "test_class"
      ))
    }

    Parameters.mergeParameters(Map(
      "dburl" -> "remote:127.0.0.1:2424/GratefulDeadConcerts",
      "user" -> "root",
      "password" -> "root",
      "class" -> "test_class"
    ))
  }
}