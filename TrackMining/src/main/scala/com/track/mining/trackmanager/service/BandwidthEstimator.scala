package com.track.mining.trackmanager.service

import scala.util.Random
import breeze.linalg._
import breeze.numerics._

case class DataPoint(x: Double, y: Double)

object BandwidthEstimator {
  def calculateDistance(p1: DataPoint, p2: DataPoint): Double = {
    math.sqrt(math.pow(p1.x - p2.x, 2) + math.pow(p1.y - p2.y, 2))
  }



  def main(args: Array[String]): Unit = {
    val dataPoints = Seq(
      DataPoint(1, 1),
      DataPoint(2, 1),
      DataPoint(1, 0),
      DataPoint(4, 7),
      DataPoint(3, 5),
      DataPoint(3, 6)
    )

    //val bandwidth = BandwidthEstimator.e(dataPoints, quantile = 0.5)
    //println(s"Estimated bandwidth: $bandwidth")
  }
}


