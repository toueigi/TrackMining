package com.track.mining.trackmanager.service

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.google.common.geometry.{S2Earth, S2LatLng}
import com.track.mining.trackmanager.bean.Point

import scala.collection.immutable.HashMap

object TrackSimilar {

  sealed trait TimePeriod

  case object July1_15 extends TimePeriod

  case object July16_31 extends TimePeriod

  case object August1_15 extends TimePeriod

  case object August16_31 extends TimePeriod

  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val gridLevelDisMetric = HashMap(12 -> 1000, 13 -> 800, 14 -> 600, 15 -> 400, 16 -> 200, 17 -> 100, 18 ->50, 19 -> 50, 20 -> 50)
  /**
    * Calculate the similarity between two time-series trajectory sequences using the DTW algorithm
    *
    * @param x         Original trajectory A
    * @param y         Compressed trajectory B
    * @param timeError    Time tolerance
    * @return Similarity
    */
  def DTW(x: Array[Point], y: Array[Point], timeError: Int = 600, gridLevel:Int = 16): Double = {
    val distanceError = if(gridLevelDisMetric.get(gridLevel).nonEmpty) gridLevelDisMetric.get(gridLevel).get else 200
    val len_x = x.size
    val len_y = y.size
    val dis = Array.ofDim[Double](len_x, len_y)
    val pathLength = Array.ofDim[Int](len_x, len_y)
    for (i <- Range(0, len_x); j <- Range(0, len_y)) {
      dis(i)(j) = 0
      pathLength(i)(j) = 0
    }
    dis(0)(0) = distanceScore(x(0), y(0), timeError, distanceError)
    pathLength(0)(0) = 1
    for (i <- Range(1, len_x)) {
      dis(i)(0) = distanceScore(x(i), y(0), timeError, distanceError) + dis(i - 1)(0)
      pathLength(i)(0) = pathLength(i - 1)(0) + 1
    }
    for (j <- Range(1, len_y)) {
      dis(0)(j) = distanceScore(x(0), y(j), timeError, distanceError) + dis(0)(j - 1)
      pathLength(0)(j) = pathLength(0)(j - 1) + 1
    }
    for (i <- Range(1, len_x); j <- Range(1, len_y)) {
      val maxElement = Array(dis(i - 1)(j), dis(i)(j - 1), dis(i - 1)(j - 1)).max
      if (maxElement == dis(i - 1)(j - 1)) {
        dis(i)(j) = dis(i - 1)(j - 1) + distanceScore(x(i), y(j), timeError, distanceError)
        pathLength(i)(j) = pathLength(i - 1)(j - 1) + 1
      }
      else if (maxElement == dis(i)(j - 1)) {
        dis(i)(j) = dis(i)(j - 1) + distanceScore(x(i), y(j), timeError, distanceError)
        pathLength(i)(j) = pathLength(i)(j - 1) + 1
      }
      else {
        dis(i)(j) = dis(i - 1)(j) + distanceScore(x(i), y(j), timeError, distanceError)
        pathLength(i)(j) = pathLength(i - 1)(j) + 1
      }
    }
    (len_x, len_y, pathLength(len_x - 1)(len_y - 1))
    dis(len_x - 1)(len_y - 1) / pathLength(len_x - 1)(len_y - 1)
  }

  /**
    * Compare the similarity between two points.
    *
    * @param x_point
    * @param y_point
    * @param timeError Time error range
    * @return
    */
  def distanceScore(x_point: Point
                    , y_point: Point
                    , timeError: Int = 600, distanceError:Int): Double = {
    val s2LatLng = S2LatLng.fromDegrees(x_point.latitude, x_point.longitude)
    val s2LatLng2 = S2LatLng.fromDegrees(y_point.latitude, y_point.longitude)
    val d = s2LatLng.getDistance(s2LatLng2, S2Earth.getRadiusMeters)
    if (Math.abs(x_point.capturetime - y_point.capturetime) < timeError) {
      if (d < distanceError) 1
      else if (d >= distanceError && d < 2 * distanceError) 0.5
      else 0
    }
    else 0
  }

  def isTimeIntersect(x: (Long, Long), y: (Long, Long)): Boolean = {
    if (x._2 < y._1 || y._2 < x._1) false
    else true
  }


  def printMatrix(x: Array[Array[Double]]): Unit = {
    for (i <- x.indices) {
      for (j <- x(0).indices) {
        print(x(i)(j).toString + " ")
      }
      println
    }
  }

  /**
    * Compile data across different cycles and calculate storage requirements, compression ratios
    * , and similarity comparisons before and after compression for various compression algorithms.
    *
    * @param records Array(device_id, date, st_track_similar, vector_track_similar
    *                , org_track_storage, st_track_storage, vector_track_storage)
    *
    * @return List[(period, track_cnt, st_track_similar_avg, vector_track_similar_avg
    *         , total_org_track_storage, total_st_track_storage, total_vector_track_storage)]
    **/
  def summarizeByPeriod(records: Array[(String, String, Double, Double, Long, Long, Long)])
  : (Array[String], Array[String]) = {
    val grouped = records
      .flatMap(record => getTimePeriod(record._2).map(period => (period, record)))
      .filter(r => (r._1 != null && r._1 != None))
      .groupBy(_._1)
      .map { case (period, periodRecords) =>
        val recordsData = periodRecords.map(_._2)
        val count = recordsData.length
        val st_track_similar_avg = recordsData.map(_._3).sum / count
        val vector_track_similar_avg = recordsData.map(_._4).sum / count
        val total_org_track_storage = recordsData.map(_._5).sum
        val total_st_track_storage = recordsData.map(_._6).sum
        val total_vector_track_storage = recordsData.map(_._7).sum

        (period, count, st_track_similar_avg, vector_track_similar_avg, total_org_track_storage, total_st_track_storage, total_vector_track_storage)
      }.toArray
    // order by date
    val periodOrder = List(July1_15, July16_31, August1_15, August16_31)
    val StorageComp = grouped.sortBy(s => periodOrder.indexOf(s._1)).map(r => formatSummary(r, "Storage Compression"))
    val CompAndPreci = grouped.sortBy(s => periodOrder.indexOf(s._1)).map(r => formatSummary(r, ""))
    (StorageComp, CompAndPreci)
  }

  /**
    * Generate formatted results based on statistical metrics
    * @param summary      Periodically aggregated data metrics
    * @param summaryType  Output data formatting type, including Storage Compression and Compression and Precision
    * @return Returns the formatted data results
    */
  def formatSummary(summary: (TimePeriod, Int, Double, Double, Long, Long, Long), summaryType: String): String = {
    val periodName = summary._1 match {
      case July1_15    => "2023/07/01 - 2023/07/15"
      case July16_31   => "2023/07/16 - 2023/07/31"
      case August1_15  => "2023/08/01 - 2023/08/15"
      case August16_31 => "2023/08/16 - 2023/08/31"
    }
    val STTPQ_ratio = "%.3f".format(summary._5.toDouble / summary._6)
    val HTV_ratio   = "%.3f".format(summary._5.toDouble / summary._7)
    val estimate = if ("Storage Compression".equals(summaryType)) {
      s"${periodName}, ${summary._2}, ${"%.3f".format(summary._5.toDouble / 1024 / 1024)} MB, ${"%.3f".format(summary._6.toDouble / 1024 / 1024)} MB, ${"%.3f".format(summary._7.toDouble / 1024 / 1024)} MB"
    } else {
      s"${periodName}, ${STTPQ_ratio}, ${HTV_ratio}, ${"%.5f".format(summary._3)}, ${"%.5f".format(summary._4)}"
    }
    estimate
  }

  /**
    * 根据日期字符串，转化为特定的周期类型
    * @param date
    * @return
    */
  def getTimePeriod(date: String): Option[TimePeriod] = {
    try {
      val formatDate = LocalDate.parse(date, dateFormatter)
      val month = formatDate.getMonthValue
      val day = formatDate.getDayOfMonth

      month match {
        case 7 =>
          if (day >= 1 && day <= 15) Some(July1_15)
          else if (day >= 16 && day <= 31) Some(July16_31)
          else None

        case 8 =>
          if (day >= 1 && day <= 15) Some(August1_15)
          else if (day >= 16 && day <= 31) Some(August16_31)
          else None

        case _ => None
      }
    } catch {
      case e: Exception =>
        println(date)
        None
    }
  }

}
