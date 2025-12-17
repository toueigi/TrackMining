package com.track.mining.trackmanager.service

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.temporal.ChronoUnit

import com.track.mining.trackmanager.bean.{LatLng, Point}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * General Methods for Trajectories
  *
  */
object PointService {
  //Earth's radius, in meters
  val EarthRadius: Double = 6371000

  /**
    * 【1】Method for removing outlier trajectory points:
    * filters outliers in sequential trajectories based on velocity and distance thresholds.
    *
    * @param trajectoryInput
    * @param speedThreshold
    * @param distanceThreshold
    * @return the trajectory filtered outliers
    */
  def removeOutlierInTrajectory(trajectoryInput: Array[Point],
                                speedThreshold: Int = 50,
                                distanceThreshold: Int = 12000
                               ): Array[Point] = {

    def divideTrunk(trajectory: Array[Point], bySpeed: Boolean):
    ListBuffer[(Int, List[Point])] = {
      val visitedSerialSet = mutable.Set[Int]() //已访问过的序号不用再做起始点
      val trunkList = new ListBuffer[(Int, List[Point])]()
      for (i <- trajectory.indices if !visitedSerialSet.contains(i)) {
        var pointA = trajectory(i)
        val trunk = new ListBuffer[Point]()
        trunk.+=: (pointA)
        for (j <- i + 1 until trajectory.length ) {
          val pointB = trajectory(j)
          val time = pointB.capturetime - pointA.capturetime
          val distance = calculateDistance(pointA.latitude, pointA.longitude, pointB.latitude, pointB.longitude)
          if(distance>3000 && pointB.capturetime ==1689388501){
            println("大距离切换")
          }
          val isConnected = if (bySpeed) distance / time < speedThreshold && distance < distanceThreshold else distance < distanceThreshold
          if (isConnected) {
            trunk.+=:(pointB)
            visitedSerialSet += j
            pointA = pointB
          }
        }
        val distinctNum = trunk.map(row => (row.latitude, row.longitude)).toSet.size
        trunkList.+=:((distinctNum, trunk.toList))
      }
      trunkList
    }

    val trajectory = trajectoryInput.distinct.sortBy(_.capturetime)
    if (trajectory.isEmpty || trajectory.length > 20000) {
      return trajectory
    }

    //When the point counts are equal, select the cluster with the larger time span.
    val bestTrunk = divideTrunk(trajectory, bySpeed = true)
      .map(r => {
        val timedif = Math.abs(r._2.head.capturetime - r._2.last.capturetime)
        (r._1, r._2, timedif)//(去重轨迹点个数，时序轨迹，跨越时长)
      }).maxBy(r => (r._1, r._3))

    val result = bestTrunk._2.toArray.sortBy(_.capturetime)
    result
  }

  /**
    * 【2】Method for removing outlier trajectory points: Define a function to identify anomalous trajectory points
    * Utilize a 2-fold variance threshold to detect potential outlier trajectory points
    *
    * @param points
    * @param threshold
    * @return the trajectory filtered outliers
    */
  def filterOutliers(points: Array[(Point)], threshold: Double = 2.0): Array[(Point)] = {
    val (mean: Double, variance: Double) = meanAndStdDev(points.map(r => new LatLng(r.latitude,r.longitude)).toSeq)
    val result = points.zipWithIndex.filter { case (point, _) =>
      val distances = points.map(p => calculateDistance(point.latitude, point.longitude, p.latitude, p.longitude))
      val avgDistance = distances.sum / distances.size
      if( avgDistance<=50 || variance <=20  ||  math.abs(avgDistance - mean) <= threshold * math.sqrt(variance))
        true
      else false
    }.map(_._1)
    result
  }

  /**
    * 【3】Haversine Formula for Calculating the Distance Between Two Points on a Sphere
    *
    * @param lat1
    * @param lon1
    * @param lat2
    * @param lon2
    * @return The Distance Between Two Points on a Sphere
    */
  def calculateDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val dLat = math.toRadians(lat2 - lat1)
    val dLon = math.toRadians(lon2 - lon1)
    val lat1toR = math.toRadians(lat1)
    val lat2toR = math.toRadians(lat2)

    val a = math.sin(dLat / 2) * math.sin(dLat / 2) +
      math.sin(dLon / 2) * math.sin(dLon / 2) * math.cos(lat1toR) * math.cos(lat2toR)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    EarthRadius * c
  }

  /**
    * 【4】Calculate the average spherical distance between all pairs of points in a set of latitudes and longitudes.
    * @param points
    * @return the average spherical distance
    */
  def calculateAverageDistance(points: Seq[LatLng]): Double = {
    if (points.length <= 1) return 0.0
    val distances = for {
      i <- points.indices
      j <- (i + 1) until points.length
    } yield calculateDistance(points(i).latitude, points(i).longitude, points(j).latitude, points(j).longitude)

    distances.sum / distances.size
  }

  /**
    * 【5】 Calculate the variance of distances within a set of latitude and longitude coordinates.
    * @param data
    * @return average distance and variance
    */
  def meanAndStdDev(data: Seq[LatLng]): (Double, Double) = {
    //【1】average distance
    val meanDistance = calculateAverageDistance(data)
    //【2】variance
      val distances = for {
        i <- data.indices
        j <- (i + 1) until data.length
      } yield calculateDistance(data(i).latitude, data(i).longitude, data(j).latitude, data(j).longitude)
    val variance = if(distances.nonEmpty && distances.size > 0)distances.map(d => math.pow(d - meanDistance, 2)).sum / distances.size else 0.0

    (meanDistance, variance)
  }

  /**
    * 【6】Calculate the distance between the two farthest points in a set of latitude and longitude coordinates.
    * @param points
    * @return  the distance between the two farthest points
    */
  def calculateMaxDistance(points: Array[Point]): Double = {
    if (points.length <= 1) return 0.0
    val distances = for {
      i <- points.indices
      j <- (i + 1) until points.length
    } yield calculateDistance(points(i).latitude, points(i).longitude, points(j).latitude, points(j).longitude)

    distances.max
  }

  /**
    * Spherical Coordinate Conversion to Cartesian Coordinates
    * @param lat
    * @param lon
    * @param R
    * @return
    */
  def latLonToXYZ(lat: Double, lon: Double, R: Double = 6371.0): (Double, Double, Double) = {
    val latRad = lat * math.Pi / 180.0
    val lonRad = lon * math.Pi / 180.0
    val x = R * math.cos(latRad) * math.cos(lonRad)
    val y = R * math.cos(latRad) * math.sin(lonRad)
    val z = R * math.sin(latRad)
    (x, y, z)
  }

  /**
    * Conversion of Cartesian Coordinates to Spherical Coordinates
    * @param x
    * @param y
    * @param z
    * @return Spherical Coordinate
    */
  def xyzToLatLon(x: Double, y: Double, z: Double): (Double, Double) = {
    val lat = math.atan2(z, math.sqrt(x * x + y * y)) * 180.0 / math.Pi
    val lon = math.atan2(y, x) * 180.0 / math.Pi
    (lat, lon)
  }

  /**
    * Spherical Distance Averaging Method
    * @param points
    * @return
    */
  def calculateCentroid(points: Seq[LatLng]): (Double, Double) = {
    var sumX = 0.0
    var sumY = 0.0
    var sumZ = 0.0
    var numPoints = 0.0

    for (point <- points) {
      val (x, y, z) = latLonToXYZ(point.latitude, point.longitude)
      sumX += x
      sumY += y
      sumZ += z
      numPoints += 1
    }

    val avgX = sumX / numPoints
    val avgY = sumY / numPoints
    val avgZ = sumZ / numPoints

    xyzToLatLon(avgX, avgY, avgZ)
  }


  /**
    * Based on the density-based clustering algorithm,
    *  track points within a specific track set that fall below a certain distance threshold are aggregated.
    * @param trajectory  the specific track set
    * @param distanceThreshold
    * @return return the center point of the specific track set
    */
  def pointCluster(trajectory: Array[Point], distanceThreshold: Double):
  Array[(Double, Double, Long)] = {
    val visitedSerialSet = mutable.Set[Int]() //已访问过的序号不用再做起始点
    val trunkList = new ListBuffer[(Double, Double, Long)]()
    for (i <- trajectory.indices if !visitedSerialSet.contains(i)) {
      var pointA = trajectory(i)
      val trunk = new ListBuffer[Point]()
      trunk.+=: (pointA)
      for (j <- i + 1 until trajectory.length ) {
        val pointB = trajectory(j)
        val time = pointB.capturetime - pointA.capturetime
        val distance = calculateDistance(pointA.latitude, pointA.longitude, pointB.latitude, pointB.longitude)
        val isConnected = if ( distance < distanceThreshold) true else false
        if (isConnected) {
          trunk.+=:(pointB)
          visitedSerialSet += j
          pointA = pointB
        }
      }
      val avgPoint  = calculateCentroid(trunk.map(r => LatLng(r.latitude, r.longitude)))
      val time = trunk.sortBy(_.capturetime).head.capturetime
      trunkList.+=:(( avgPoint._1, avgPoint._2, time ))
    }
    trunkList.toArray
  }


  /**
    * Methods for Generating Simulated Trajectory Data
    * @param device_id simulated device id
    * @param dateStr date
    * @return simulated trajectory data
    */
  def generateTrajectoryData(device_id: String, dateStr: String): Point = {
    val minLng: Double = 70.0
    val maxLng: Double = 136.0
    val minLat: Double = 3
    val maxLat: Double = 55

    val latitude  =  Random.nextDouble() * (maxLat - minLat) + Random.nextDouble() * minLat  // -90 to 90
    val longitude =  Random.nextDouble() * (maxLng - minLng) + Random.nextDouble() * minLng// -180 to 180
    val altitude = Random.nextFloat() * 10000 // 0 to 10000 meters
    val speed = Random.nextFloat() * 100 // 0 to 100 km/h
    val data_type = 0 // 假设所有数据都是GPS类型
    val date = generateDateTime(dateStr) // 当前日期时间

    Point(device_id, latitude, longitude, altitude, speed, data_type, date)
  }


  /**
    * randomly generate a date and time field for a specified date
    * @param starDateStr specified date
    * @return DateTime
    */
  def generateDateTime(starDateStr:String):String = {
    // 定义开始和结束日期
    val startDate = LocalDate.of(starDateStr.substring(0,4).toInt, starDateStr.substring(4,6).toInt, starDateStr.substring(6,8).toInt)
    // 随机生成一天中的时间
    val randomTime = LocalTime.of(
      Random.nextInt(24), // 小时
      Random.nextInt(60)  // 分钟
    )
    // 组合日期和时间
    val randomDateTime = LocalDateTime.of(startDate, randomTime)
    // 定义日期时间格式
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    // 格式化日期时间为字符串
    val formattedDateTime: String = randomDateTime.format(formatter)
    formattedDateTime
  }

  def main(args: Array[String]): Unit = {
    println(generateDateTime("20241231"))
  }
}

