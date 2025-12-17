package com.track.mining.trackmanager

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.google.common.geometry.{MyS2Encode, S2LatLng}
import com.track.mining.algorithm.lsh.{VectorLSH, VectorLSHModel}
import com.track.mining.common.Logging
import com.track.mining.trackmanager.TrackDataSimilar.{log, logError, logInfo, s2_level}
import com.track.mining.trackmanager.bean.Point
import com.track.mining.trackmanager.service.{MysqlTrackService, PointService, TrackLSHService}

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer

/**
  * This section covers the relevant statistical indicators from Chapter 5.4.
  */

object TrackDataLSH extends Logging {

  val outFile: File = new File("data\\output\\chapter5.4\\tables")

  def main(args: Array[String]): Unit = {
//    println(getB(4, 0.4, 0.95))
    analyse("20230720", "20230725" )
  }

  /**
    * Calculate the number of pairs of relationships with similar travel patterns existing each day within the specified date range.
    *
    * @param startDate 20230726
    * @param endDate   20230825
    */
  def analyse(startDate:String, endDate:String) = {

    val filterTags = Array("0, 1000", "0, 10000", "0, 50000")
    // retrievalRs data struct: ArrayBuffer[(Date, DeviceA, DeviceB, Trajectory_Similarity, Algorithms_Type)]
    val retrievalRs = new ArrayBuffer[(String, String, String, Double, String)]()
    // costTimes data struct: ArrayBuffer[(Device_Cnt, Array[(Date, LSHCost, JaccardCost)]
    val costTimes   = new ArrayBuffer[(Int, Array[(String, Long, Long)] )]

    //Calculate separately based on the input device volume parameters.
    filterTags.foreach(filterTag => {
      val LSHRS = new ArrayBuffer[(String, String, String, Int)]()
      //return the result of jaccard and costTime
      val (jaccardRs, costTime) = TrackLSHService.groupCommMovementAnalyse(LSHRS, startDate, endDate, filterTag, moveMinDistanceFilter = 2000.0
        , distinctGeoCnt = 5, timespan = 120, computeBruteForce = 1)
      // union the results of lsh and jaccard.
      LSHRS.distinct.map(r => {
        ((r._1, r._2, r._3), (0.0, "LSH"))
      }).union(jaccardRs.map(r => ((r._1, r._2, r._3), (r._4,"JACCARD"))))
        .groupBy(_._1)
        .mapValues(r => (r.map(_._2._1).max, r.map(_._2._2).mkString("#")))
        .toArray
        .sortBy(_._1._3)
        .map(r => retrievalRs.append((r._1._3, r._1._1, r._1._2, r._2._1, r._2._2)) )
      //append the cost time of dif device volume parameters
      costTimes.append((filterTag.split(",")(1).trim.toInt, costTime.toArray))
    })
    //output the results of lsh and jaccard.
    val outRS = retrievalRs.distinct.map(row => s"${row._1}, ${row._2}, ${row._3}, ${row._4}, ${row._5}")
    DataWarehouse.saveAsTxtFile(outFile + File.separator + s"Table11_LSH_Hits_${startDate}_${endDate}.txt", outRS.toArray)

    //output the time consumption of LSH and brute-force computation methods under different device parameters
    val outCost = costTimes.flatMap(row =>{
      row._2.map(r => (r._1, (r._2, r._3, row._1)))
    }).groupBy(_._1)
        .map(r => {
          val sortRS = r._2.sortBy(_._2._3)
          (
             ( r._1, "LSH-TTSS Time (ms)   ", sortRS.map(_._2._1).mkString(", ") )
            ,( r._1, "Brute-force Time (ms)", sortRS.map(_._2._2).mkString(", ") )
            )
        }).toArray
    val allCost = (outCost.map(_._1) ++ outCost.map(_._2)).sortBy(r => (r._1, r._2.reverse)).map(r => (s"${r._1}, ${r._2}, ${r._3}"))
    DataWarehouse.saveAsTxtFile(outFile + File.separator + s"Table12_Time_Efficiency_Across_Different_Algorithms_and_Dataset_Sizes_${startDate}_${endDate}.txt", allCost)
  }

  /**
    * Verify the LSH algorithm using a single test device
    */
  def analyseLSHSingle():Unit = {
    val track = TrackLSHService.generateTestTrackData(16)
    println(s"Number of target trajectories obtained：${track.size}")
    val lsh  = TrackLSHService.LSHCore(track)
  }

  /**
    * Calculate the number of buckets b required such that,
    * for each bucket containing r hash functions, the probability that two sets satisfying similarity s
    * equal to p are assigned to the same bucket and all r hash values in that bucket are equal is achieved.
    * @return  numbers of bucket
    */
  def getB(r: Int, s: Double, p: Double): Long = {
    Math.round(Math.log(1 - p) / Math.log(1 - Math.pow(s, r)))
  }

}
