package observatory

import java.time.LocalDate

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

trait ExtractionTest extends FunSuite {

  test("empty all") {
    val year = 0
    val stations = Extraction.spark.sparkContext.parallelize(List(""))
    val temperatures = Extraction.spark.sparkContext.parallelize(List(""))

    var computed = Extraction.locateTemperaturesSpark(year, stations, temperatures).collect()
    val expected = Array[(LocalDate, Location, Temperature)]()

    assert(computed.sameElements(expected))
  }

  test("empty stations") {
    val year = 0
    val stations = Extraction.spark.sparkContext.parallelize(List(""))
    val temperatures = Extraction.spark.sparkContext.parallelize(List("4,,01,01,32"))

    var computed = Extraction.locateTemperaturesSpark(year, stations, temperatures).collect()
    val expected = Array[(LocalDate, Location, Temperature)]()

    assert(computed.sameElements(expected))
  }

  test("empty temperatures") {
    val year = 0
    val stations = Extraction.spark.sparkContext.parallelize(List("4,,+1,+1"))
    val temperatures = Extraction.spark.sparkContext.parallelize(List(""))

    var computed = Extraction.locateTemperaturesSpark(year, stations, temperatures).collect()
    val expected = Array[(LocalDate, Location, Temperature)]()

    assert(computed.sameElements(expected))
  }

  test("both nonempty") {
    val year = 0
    val stations = Extraction.spark.sparkContext.parallelize(List("4,,+1,+2", "4,5,+1,+2"))
    val temperatures = Extraction.spark.sparkContext.parallelize(List("4,,01,01,32", "4,10,01,01,32"))

    var computed = Extraction.locateTemperaturesSpark(year, stations, temperatures).collect()
    val expected = Array((LocalDate.of(year, 1, 1), Location(1, 2), 0))

    assert(computed.sameElements(expected))
  }
}