package com.track.mining.trackmanager

import java.io.File

import com.track.mining.algorithm.lsh.VectorLSHModel
import com.track.mining.common.Logging
import com.track.mining.trackmanager.service.TrackLSHService

import scala.collection.mutable.ArrayBuffer

/**
  * This class pertains to relevant statistical metrics from Experiment 4.
  */
object TrackPatternsDetection extends Logging {

  val outFile: File     = new File("data\\output\\chapter6\\tables")
  val outViewFile: File = new File("data\\output\\chapter6\\figures")

  def main(args: Array[String]): Unit = {
    //【1】groupCommMovementAnalyse
    analyse(0)
    //【2】personalFrequentAnalyse
    analyse(1)
    //【3】generate the view
    generateView()
  }

  /**
    * Calculate the number of pairs of relationships with similar travel patterns existing each day within the specified date range.
    * @param startDate 20230726
    * @param endDate   20230825
    */
  def analyse(analyseType:Int, startDate:String = "20230701", endDate:String = "20230901", filterTag:String = "0,0") = {

    val LSHRS = new ArrayBuffer[(String, String, String, Int)]()

    if(analyseType == 0){
      //【1】GroupCommMovementAnalyse Entrance
      TrackLSHService.groupCommMovementAnalyse(LSHRS, startDate, endDate, filterTag, timespan = 120, s2_level = 16)
      val rs = LSHRS.distinct.map(r => {
        ((r._1,r._2),r._3)
      }).groupBy(_._1)
        .toArray
        .sortBy(r => (-r._2.size,r._1._1))
        .map(r => (s"${r._1._1},${r._1._2},${r._2.size},${r._2.map(_._2).mkString("#")}"))
      DataWarehouse.saveAsTxtFile(outFile + File.separator + s"Table13_Co-Movement_Device_Pairs_among_Elderly_Volunteers${startDate}_${endDate}.txt", rs)
    }else{
      //【2】PersonalFrequentAnalyse Entrance
      val tagetTrackDetail = personalFrequentAnalyse(LSHRS, startDate, endDate, filterTag, timespan = 1500, s2_level = 12)
      log.info(s"Completion Date Range: ${startDate}-${endDate} Identification of frequent personal trajectories within the specified period: A total of ${LSHRS.size} trajectories were identified!")
      // Find all records with shared dates
      val mergeResult = LSHRS.distinct.map(row => {
        (row._1, row._3.split("#").toSet)
      }).groupBy(_._1)
        .flatMap{ case (device, dateSets) =>
          val allDates = dateSets.map(_._2)
          // merge all date
          val merged = mergeDateSets(allDates.toList)
          merged.map { dateSet =>
            val sortedDates = dateSet.toArray.map(row => {
              val deviceInfo = s"$device#${row}"
              (deviceInfo, tagetTrackDetail.get(deviceInfo).get)
            })
            val filterRs = personalFrequentJaccard(sortedDates, filterSimi = 0.4)
            val dates = filterRs.map(_._1.split("#")(1))
            (device, dates.distinct.sorted, dates.distinct.size)
          }.filter(_._3 >= 3 )
        }
      // output the result
      val result = mergeResult.toArray.sortBy(r => (-r._3, r._1) )
        .map(row => s"${row._1},${row._2.mkString("#")},${row._3}")
      DataWarehouse.saveAsTxtFile(outFile + File.separator + s"Table14_Distribution_of_Devices_Exhibiting_Individual_Frequent_Movement_Patterns_${startDate}_${endDate}.txt", result)
    }

    log.info(s"Finish from ${startDate} to ${endDate} similar devices analyse! ")
  }

  /**
    * personalFrequentAnalyse entrance method
    * @param resultArray The cached result object, with the result being:
    *                    ArrayBuffer[(device_id, device_id, dates.distinct.mkString("#"), dates.distinct.size)]
    * @param startDate   Query Start Date
    * @param endDate     Query End Date
    * @param filterTag   Filter device designation.
    *                   “0,0” indicates querying the trajectory of real device numbers;
    *                   “0,999” indicates querying the trajectory of both real and simulated device numbers.
    * @return
    */
  def personalFrequentAnalyse(resultArray:ArrayBuffer[(String, String, String, Int)],startDate:String, endDate:String
                              , filterTag:String, timespan:Int, s2_level:Int )
  :Map[String, Array[Long]] = {
    //Load all trajectory data for senior volunteers and return：Array(日期,(deviceid, Array(track_code)))
    val volunteerDeviceTrack =
      TrackLSHService.generateAllTrackVector(s"${startDate},${endDate}", filterTag = "0,0", moveMinDistanceFilter= 2000.0
        , distinctS2GridCnt = 3, timespan, s2_level)
        .map(row => (row._2._1,(row._1,row._2._2)))
        .groupBy(_._1)
        .mapValues(row => {
          row.map(r => (r._1+"#"+r._2._1,r._2._2))//merge the device_id and date
        })
    //List of results stored for LSH: ArrayBuffer(device, lshRs, Array(vectorId, deviceId))
    val lshList = new ArrayBuffer[( String, VectorLSHModel, Array[(Int,String)] )]

    //Store the trajectory information for the target day
    val targetTrackDetail = new ArrayBuffer[(String, Array[Long])]//Array(filterDate,(deviceid, Array(track_code)))

    //return ：Array(deviceid,(date, Array(track_code)))
    volunteerDeviceTrack.foreach(row => {
      val lsh = TrackLSHService.LSHCore(row._2, numRows = 65 * 4, numBands = 65 )
      //return :(device, lshRs, Array(vectorIdx, deviceid#date))
      lshList.append((row._1, lsh._1 , lsh._2))
    })

    val deviceDateWithTrack = volunteerDeviceTrack.toArray.flatMap(row => {
      row._2
    }).toMap

    //【2】Identify device clusters exhibiting similar daily patterns
    lshList.sortBy(_._1).foreach(row =>{
      val vectorLSHModel = row._2
      if(vectorLSHModel.cluster_vector.size > 0){
        val id2Index = row._3.toMap //设备的编码和编码源码（手机号码）的对应关系

        vectorLSHModel.cluster_vector.groupBy(_._1).map { case (k, v) => {
          var device_id = ""
          val dates = new ArrayBuffer[String]
          v.map(_._2).foreach(elem => {
            val device_info = id2Index.get(elem).get
            device_id = device_info.split("#")(0)
            dates.append(device_info.split("#")(1))
            val track_codes = deviceDateWithTrack.get(device_info).get
            targetTrackDetail.append( (device_info, track_codes) )
          })
          if(dates.size > 1){
            resultArray.append((device_id, device_id, dates.distinct.mkString("#"), dates.distinct.size))
          }
        }}
      }
    })
    targetTrackDetail.toMap
  }

  /**
    * Using a universal brute-force comparison algorithm, calculate the similarity between each pair.
    *
    * @param allTargetTrackVecotorRdd Vectorized trajectory data of the target: Array(device_info, Array(track_code))
    * @param filterSimi the min similarity
    * @return
    */
  def  personalFrequentJaccard(allTargetTrackVecotorRdd: Array[(String, Array[Long])] ,filterSimi:Double = 0.4):Array[(String, Int)] = {
    val statTime = System.currentTimeMillis()
    val jaccardSimilar = new ArrayBuffer[(String, String, Double)]
    allTargetTrackVecotorRdd.combinations(2).toArray.map(row =>{
      val ret = TrackLSHService.jaccard(row(0), row(1))
      jaccardSimilar.append( (ret._1, ret._2, ret._3 ))
    })
    val result = (jaccardSimilar ++ jaccardSimilar.map(r => (r._2,r._1,r._3))).distinct
      .groupBy(_._1)
      .map(row =>{
        val filterRS = row._2.filter(_._3 >= filterSimi)
        (row._1, filterRS.size)
      }).filter(_._2 >= 2).toArray
    result
  }

  /**
    * Date clustering aggregates data containing the same date to reduce volatility.
    * @param dateSets
    * @return
    */
  def mergeDateSets(dateSets: List[Set[String]]): List[Set[String]] = {
    def mergeIfIntersect(sets: List[Set[String]]): List[Set[String]] = {
      sets match {
        case Nil => Nil
        case head :: tail =>
          val (connected, disconnected) = tail.partition(_.intersect(head).nonEmpty)
          if (connected.isEmpty) {
            head :: mergeIfIntersect(disconnected)
          } else {
            val merged = head ++ connected.flatten
            mergeIfIntersect(merged :: disconnected)
          }
      }
    }
    mergeIfIntersect(dateSets)
  }

  /**
    * Generate a scatter plot of trajectory points for a specific number on a specific date
    *
    */
  def generateView():Unit ={

    val info  =  Array(
      ("13312135675", "2023-07-16,2023-07-17", "Co-Movement")
      , ("15512328790", "2023-07-16,2023-07-17", "Co-Movement")
      , ("18725467798", "2023-07-16,2023-07-17", "Co-Movement")
      , ("18922289760", "2023-07-16,2023-07-17", "Co-Movement")
      , ("15820237868", "2023-07-13,2023-07-14", "Co-Movement")
      , ("18923009087", "2023-07-13,2023-07-14", "Co-Movement")
      , ("18820976587", "2023-07-21, 2023-07-22", "Individual_Movement")
      , ("18820976587", "2023-07-26, 2023-07-27", "Individual_Movement")
      , ("13987602543", "2023-08-03, 2023-08-04", "Individual_Movement")
      , ("13987602543", "2023-08-06, 2023-08-07", "Individual_Movement")
      , ("13312135675", "2023-07-19, 2023-07-20", "Individual_Movement")
      , ("13312135675", "2023-07-20, 2023-07-21", "Individual_Movement")
    )
    info.map(row => {
      if("Co-Movement".equals(row._3))
        TrackDataCompress.TrackProductQuantization(queryTarget = row._1, dateCond = row._2, travelModes= row._3, generateQuantiView = true, generateOrigView=false, outViewFilePath = outViewFile)
      else
        TrackDataCompress.TrackProductQuantization(queryTarget = row._1, dateCond = row._2, travelModes= row._3, generateQuantiView = false, generateOrigView=true, outViewFilePath = outViewFile)

    })

  }
}
