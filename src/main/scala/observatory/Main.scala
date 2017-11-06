package observatory

import org.apache.log4j.{Level, Logger}

object Main extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val temps = Extraction.locateTemperatures(1975, "/stations.csv", "/1975.csv")

  println(temps)

}
