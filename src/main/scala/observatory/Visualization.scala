package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import scala.math.{abs, sin, cos, sqrt, pow, toRadians, asin}

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  val p_param = 6
  val width = 360
  val height = 180

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val dists = temperatures.map(entry => (computeDist(location, entry._1), entry._2))

    val min = dists.reduce((a, b) => if(a._1 < b._1) a else b)
    if(min._1 < 1) {
      // a close enough station (< 1km), take its temperature
      min._2
    }else{
      // interpolate
      val weights = dists.map(entry => (1 / pow(entry._1, p_param), entry._2))
      val normalizer = weights.map(_._1).sum
      weights.map(entry => entry._1 * entry._2).sum  / normalizer
    }
  }

  /**
    * @param a First location
    * @param b Second location
    * @return Are the locations antipodes?
    */
  def areAntipodes(a: Location, b: Location): Boolean = {
    (a.lat == -b.lat) && (abs(a.lon - b.lon) == 180)
  }

  /**
    * @param a First location
    * @param b Second location
    * @return Great-circle distance between a and b
    */
  def computeDist(a: Location, b: Location): Double = {
    if(a == b){
      0
    }else if(areAntipodes(a, b)){
      earthRadius * math.Pi
    } else {
      val delta_lon = toRadians(abs(a.lon - b.lon))
      val alat = toRadians(a.lat)
      val blat = toRadians(b.lat)
      val delta_lat = abs(alat - blat)
      val delta_sigma =   2 * asin(sqrt( pow(sin(delta_lat/2), 2) + cos(alat) * cos(blat) * pow(sin(delta_lon/2), 2) ))
      earthRadius * delta_sigma
    }
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    val sameCol = points.find(_._1 == value)
    sameCol match {
      case Some((_, col)) => col
      case _ =>
        val (smaller, bigger) = points.partition(_._1 < value)
        if (smaller.isEmpty) {
          bigger.minBy(_._1)._2
        }else {
          val a = smaller.maxBy(_._1)
          if (bigger.isEmpty) {
            a._2
          }else {
            val b = bigger.minBy(_._1)
            val wa = 1 / abs(a._1 - value)
            val wb = 1 / abs(b._1 - value)
            def interp(x: Int, y: Int): Int =
              ((wa * x + wb * y) / (wa + wb)).round.toInt
            val ca = a._2
            val cb = b._2
            Color(interp(ca.red, cb.red), interp(ca.green, cb.green), interp(ca.blue, cb.blue))
          }
        }
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val coords = for {
      i <- 0 until height
      j <- 0 until width
    } yield (i, j)
    val pixels = coords.par
      .map(transformCoord)
      .map(predictTemperature(temperatures, _))
      .map(interpolateColor(colors, _))
      .map(col => Pixel(col.red, col.green, col.blue, 255))
      .toArray
    Image(width, height, pixels)
  }

  /**
    * @param coord Pixel coordinates
    * @return Latitude and longitude
    */
  def transformCoord(coord: (Int, Int)): Location = {
    val lon = (coord._2 - width/2) * (360 / width)
    val lat = -(coord._1 - height/2) * (180 / height)
    Location(lat, lon)
  }
}

