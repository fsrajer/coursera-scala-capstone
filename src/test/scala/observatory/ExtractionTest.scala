package observatory

import java.time.LocalDate

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import Extraction.spark

trait ExtractionTest extends FunSuite {

  test("empty all") {
    val year = 0
    val stations = spark.sparkContext.parallelize(List(""))
    val temperatures = spark.sparkContext.parallelize(List(""))

    var computed = Extraction.locateTemperaturesSpark(year, stations, temperatures).collect()
    val expected = Array[(LocalDate, Location, Temperature)]()

    assert(computed.sameElements(expected))
  }

  test("empty stations") {
    val year = 0
    val stations = spark.sparkContext.parallelize(List(""))
    val temperatures = spark.sparkContext.parallelize(List("4,,01,01,32"))

    var computed = Extraction.locateTemperaturesSpark(year, stations, temperatures).collect()
    val expected = Array[(LocalDate, Location, Temperature)]()

    assert(computed.sameElements(expected))
  }

  test("empty temperatures") {
    val year = 0
    val stations = spark.sparkContext.parallelize(List("4,,+1,+1"))
    val temperatures = spark.sparkContext.parallelize(List(""))

    var computed = Extraction.locateTemperaturesSpark(year, stations, temperatures).collect()
    val expected = Array[(LocalDate, Location, Temperature)]()

    assert(computed.sameElements(expected))
  }

  test("both nonempty") {
    val year = 0
    val stations = spark.sparkContext.parallelize(List("4,,+1,+2", "4,5,+1,+2"))
    val temperatures = spark.sparkContext.parallelize(List("4,,01,01,32", "4,10,01,01,32"))

    var computed = Extraction.locateTemperaturesSpark(year, stations, temperatures).collect()
    val expected = Array((LocalDate.of(year, 1, 1), Location(1, 2), 0))

    assert(computed.sameElements(expected))
  }

  test("average empty") {
    val records = spark.sparkContext.parallelize(List[(LocalDate, Location, Temperature)]())

    var computed = Extraction.locationYearlyAverageRecordsSpark(records).collect()
    val expected = Array[(Location, Temperature)]()

    assert(computed.sameElements(expected))
  }

  test("average non-empty") {
    val year = 4
    val records = spark.sparkContext.parallelize(
      List[(LocalDate, Location, Temperature)]
        ((LocalDate.of(year, 1, 1), Location(1, 2), 4),
          (LocalDate.of(year, 4, 1), Location(1, 2), 5),
          (LocalDate.of(year, 1, 5), Location(1, 2), 6),
          (LocalDate.of(year, 1, 1), Location(2, 2), 4)))

    var computed = Extraction.locationYearlyAverageRecordsSpark(records).collect().toSet
    val expected = Set[(Location, Temperature)]((Location(1, 2), 5), (Location(2, 2), 4))

    assert(computed == expected)
  }
}