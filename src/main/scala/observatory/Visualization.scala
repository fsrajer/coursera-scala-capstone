package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.spark.rdd.RDD

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  val p_param = 2

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    predictTemperatureSpark(spark.sparkContext.parallelize(temperatures.toSeq), location)
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
      Math.PI
    } else {
      val delta_lon = Math.abs(a.lon - b.lon)
      val delta_sigma = Math.acos(
        Math.sin(a.lat) * Math.sin(b.lat) + Math.cos(a.lat) * Math.cos(b.lat) * Math.cos(delta_lon))
      earthRadius * delta_sigma
    }
  }

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperatureSpark(temperatures: RDD[(Location, Temperature)], location: Location): Temperature = {
    val dists = temperatures.map(entry => (computeDist(entry._1, location), entry._2)).cache()

    val min = dists.reduce((a, b) => if(a._1 < b._1) a else b)
    if(min._1 < 1) {
      // a close enough station (< 1km), take its temperature
      min._2
    }else{
      // interpolate
      val weights = dists.map(entry => (1 / Math.pow(entry._1, p_param), entry._2)).cache()
      val sum = weights.keys.sum()
      weights.map(entry => (entry._1 * entry._2) / sum).sum()
    }
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    ???
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    ???
  }

}

