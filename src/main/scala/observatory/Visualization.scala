package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.spark.rdd.RDD

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  val p_param = 2
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
      val weights = dists.map(entry => (1 / math.pow(entry._1, p_param), entry._2))
      val sum = weights.map(_._1).sum
      weights.map(entry => (entry._1 * entry._2) / sum).sum
    }
  }

  /**
    * @param a First location
    * @param b Second location
    * @return Are the locations antipodes?
    */
  def areAntipodes(a: Location, b: Location): Boolean = {
    a.lat == -b.lat && (a.lon == b.lon || a.lon == -b.lon)
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
      math.Pi
    } else {
      val delta_lon = math.abs(a.lon - b.lon) * math.Pi / 180
      val alat = a.lat * math.Pi / 180
      val blat = b.lat * math.Pi / 180
      val delta_sigma = math.acos(
        math.sin(alat) * math.sin(blat) + math.cos(alat) * math.cos(blat) * math.cos(delta_lon))
      earthRadius * delta_sigma
    }
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    val sameCol = points.filter(_._1 == value)
    if(sameCol.nonEmpty){
      sameCol.head._2
    }else{
      val smaller = points.filter(_._1 < value)
      val bigger = points.filter(_._1 > value)
      if (smaller.isEmpty) {
        bigger.minBy(_._1)._2
      }else {
        val a = smaller.maxBy(_._1)
        if (bigger.isEmpty) {
          a._2
        }else {
          val b = bigger.minBy(_._1)
          val wa = 1 / math.abs(a._1 - value)
          val wb = 1 / math.abs(b._1 - value)
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
    Location(lon, lat)
  }
}

