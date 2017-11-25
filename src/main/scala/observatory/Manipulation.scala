package observatory

/**
  * 4th milestone: value-added information
  */
object Manipulation {

  class Grid {
    private var temps: Array[Temperature] = new Array[Temperature](360*180)

    private def address(lat: Int, lon: Int): Int = {
      val x = lon + 180
      val y = lat + 89
      y * 360 + x
    }

    def set(lat: Int, lon: Int, temp: Temperature): Unit = {
      temps(address(lat, lon)) = temp
    }

    def get(lat: Int, lon: Int): Temperature = {
      temps(address(lat, lon))
    }

    def precompute(temps: Iterable[(Location, Temperature)]): Unit = {
      for {
        lat <- Range(90, -90, -1)
        lon <- -180 until 180
      } set(lat, lon, Visualization.predictTemperature(temps, Location(lat, lon)))
    }

    def +=(that: Grid): Grid = {
      temps.indices.foreach(idx => this.temps(idx) += that.temps(idx))
      this
    }

    def /=(denominator: Double): Grid = {
      temps = temps.map(_ / denominator)
      this
    }

    def -=(that: GridLocation => Temperature): Grid = {
      for {
        lat <- Range(90, -90, -1)
        lon <- -180 until 180
      } set(lat, lon, get(lat, lon) - that(GridLocation(lat, lon)))
      this
    }
  }

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    val grid = makeGridInstance(temperatures)
    (gl: GridLocation) => grid.get(gl.lat, gl.lon)
  }

  def makeGridInstance(temperatures: Iterable[(Location, Temperature)]): Grid = {
    val grid = new Grid
    grid.precompute(temperatures)
    grid
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    val grid = temperaturess.par
      .map(makeGridInstance)
      .reduce((a: Grid, b: Grid) => a += b)
    grid /= temperaturess.size
    (gl: GridLocation) => grid.get(gl.lat, gl.lon)
  }

  /**
    * @param temperatures Known temperatures
    * @param normals A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    val grid = makeGridInstance(temperatures)
    grid -= normals
    (gl: GridLocation) => grid.get(gl.lat, gl.lon)
  }


}

