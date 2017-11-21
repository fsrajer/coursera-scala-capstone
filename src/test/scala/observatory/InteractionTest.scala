package observatory

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

import scala.collection.concurrent.TrieMap

import Interaction._

trait InteractionTest extends FunSuite with Checkers {

  test("tileLocation test") {
    val tile = Tile(0, 0, 0)
    val computed = tileLocation(tile)
    val expected = Location(85.0511, -180)

    assert(computed === expected)
  }

}
