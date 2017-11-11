package observatory


import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

class QuickCheckVisualization extends Properties("Visualization") {

  lazy val genLocation: Gen[Location] = for {
    lat <- Gen.choose[Double](-90, 90)
    lon <- Gen.choose[Double](-180, 180)
  } yield Location(lat, lon)
  implicit lazy val arbLocation: Arbitrary[Location] = Arbitrary(genLocation)

  property("dist valid") = forAll { (x: Location, y: Location) =>
    Visualization.computeDist(x, y) < (earthRadius * 2 * Math.PI)
  }
}
