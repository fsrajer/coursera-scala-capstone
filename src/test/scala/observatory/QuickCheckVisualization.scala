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

  property("predictTemperature two locations") = forAll {
    (x: Location, y: Location, z: Location, x_temp: Double, y_temp: Double) =>
      val z_temp = Visualization.predictTemperature(List((x, x_temp), (y, y_temp)), z)
      val xz_dist = Visualization.computeDist(x, z)
      val yz_dist = Visualization.computeDist(y, z)
      val temp_mid = (x_temp+y_temp)/2
      if(xz_dist < yz_dist) {
        if(x_temp < y_temp) {
          z_temp < temp_mid
        } else {
          z_temp > temp_mid
        }
      } else {
        if(y_temp > x_temp) {
          z_temp > temp_mid
        } else {
          z_temp < temp_mid
        }
      }
  }

}
