package com.track.mining.trackmanager

import java.io.{File, PrintWriter}

import com.google.common.geometry.{MyS2Encode, S2CellId, S2LatLng}
import com.track.mining.common.{Logging, TrackCoordinateConverter}
import com.track.mining.trackmanager.bean.{LatLng, Point}
import com.track.mining.trackmanager.service.MysqlTrackService.{dateToTimestamp, level}
import com.track.mining.trackmanager.service.{DuckDBTrackService, MysqlTrackService, PointService, TrackSimilar}

import scala.collection.mutable.ArrayBuffer


/**
  * Comparison of trajectory data before and after quantitative processing, highlighting that:
  * Trajectories remain distortion-free post-quantification while data volume is reduced.
  * This section covers the relevant statistical indicators and figures from Chapter 5.2.
  */
object TrackDataCompress extends Logging {
  val outStatFile: File     = new File("data\\output\\chapter5.2\\tables")
  val outViewFile: File     = new File("data\\output\\chapter5.2\\figures")
  val viewHtmlTemplate:File = new File("data\\template\\template.html")

  def main(args: Array[String]): Unit = {

    STTPQ4DifTravelModes()

  }

  /**
    * STTPQ Compression Effectiveness Across Different Travel Modes
    */
  def STTPQ4DifTravelModes():Unit = {
    val fileName = "Table7_Reduction_Ratio_of_STTPQ_for_Different_Travel_Modes"
    createFile(outStatFile)
    createFile(outViewFile)
    val metricInfo = new ArrayBuffer[(Int, Int, Double,String)]()
    /***********************************Output compressed comparison results for different modes of transportation**************************/
    //18820976587,2023/7/22 -- Walking
    //18924009080,2023/7/15 -- Subway
    //13890976542,2023/7/21 -- Driving
    TrackProductQuantization("18820976587", dateCond ="2023-07-22,2023-07-23", metricInfo, "Walking")
    TrackProductQuantization("18924009080", dateCond ="2023-07-15,2023-07-16", metricInfo, "Subway")
    TrackProductQuantization("13890976542", dateCond ="2023-07-21,2023-07-22", metricInfo, "Driving")
    var index = 0
    val metricRs = metricInfo.map(metric => {
      val compress = "%.4f".format((metric._1 - metric._2)/metric._1.toFloat)
      s"${metric._4}, ${metric._1}, ${metric._2}, ${compress}"
      }).toArray
    DataWarehouse.saveAsTxtFile(s"${outStatFile+File.separator+fileName}", metricRs)
  }

  /**
    * Comparison of before and after effects after quantization of time-series trajectory
    * @param queryTarget
    * @param dateCond eg:dateCond:String =“2023-07-15, 2023-07-16”
    * @param metricInfo
    * @param travelModes
    * @param generateQuantiView
    * @param generateOrigView
    * @param outViewFilePath
    */
  def TrackProductQuantization(queryTarget:String = "",dateCond:String ="2023-07-15, 2023-07-16"
                               , metricInfo:ArrayBuffer[(Int, Int, Double, String)] = null, travelModes:String = ""
                               , generateQuantiView:Boolean = true, generateOrigView:Boolean = true, outViewFilePath:File = null ): Unit = {
    val  viewFilePath = if(outViewFilePath == null) this.outViewFile else outViewFilePath
    val txtFileName = if(queryTarget.nonEmpty) queryTarget else "All"
    val beginDate   = dateCond.split(",").head
    val orgTrackFileName       = travelModes + File.separator + txtFileName+"_"+beginDate+"_orig"
    val quantiTrackFileName    = travelModes + File.separator + txtFileName+"_"+beginDate+"_quantify"
    //【1】Load trajectory data for the specified query period.
    // return：ArrayBuffer[(device_id, capturetime,lngitude,latitude)]
    val targetTrack = MysqlTrackService.dataConvertSpaceTime(queryTarget, dateCond, false).sortBy(_.capturetime)

    val moveOutTrack = PointService.removeOutlierInTrajectory(targetTrack.distinct.toArray)
      .map(row =>
        new Point(device_id = row.device_id, latitude = row.latitude, longitude = row.longitude, row.altitude, row.speed, 2, row.date,
          s2cell = row.s2cell,capturetime = row.capturetime , timespan = row.timespan)
      )

    // 【2】Perform spatio-temporal slicing of time-series trajectory data
    //  return ：List[((device_id,timespan),Array((lngitude,latitude), S2CellId))]
    val track2ST = moveOutTrack
      .map(row => {
        val wholeDay = MysqlTrackService.getDayTimestampFromDate(row.date.substring(0, 10))
        ( (row.device_id, ( row.capturetime - wholeDay._2 )/120), row)
      }).sortBy(_._1._2)

    //【3】Perform product quantization on the spatiotemporal trajectory
    val track2SpaceTime = track2ST
      .groupBy(_._1)
      .mapValues(points => {

        val filterPoints = PointService.filterOutliers(points.map(_._2))
        if (filterPoints.isEmpty) {
          logError("Trajectory filtering error！")
          points.map(row => s"${row._1._1}, ${row._1._2}, ${row._2.latitude}，${row._2.longitude}").map(print(_))
        }
        filterPoints.map(filterPoint => {
          val latLng: S2LatLng = S2LatLng.fromDegrees(filterPoint.latitude, filterPoint.longitude)
          (S2CellId.fromLatLng(latLng).parent(level), filterPoint)
        }).groupBy(_._1)
          .map(row => {
            val centerPoint = PointService.calculateCentroid(row._2.map(r => new LatLng(r._2.latitude, r._2.longitude)))
            val minCapturetime = row._2.map(_._2.capturetime).min
            (centerPoint._1, centerPoint._2, minCapturetime, row._1)
          }).toArray
      })
      .flatMap(tracks => {
        tracks._2.map(point => {
          new Point(device_id = tracks._1._1, latitude = point._1, longitude = point._2, 0.0f, 0.0f, 1, "",
            s2cell = point._4.id(), point._3, timespan = tracks._1._2)
        })
      })
      .toArray
      .sortBy(_.capturetime)

    //【4】Output Product Quantization Compression Trajectory
    track2SpaceTime.sortBy(_.capturetime).map(row => s"${row.latitude},${row.longitude},${row.timespan},${row.capturetime}").map(println(_))

    val similar = TrackSimilar.DTW(targetTrack.sortBy(_.capturetime).toArray, track2SpaceTime,120)
    println(s"The similarity before and after trajectory compression is：${similar}")


    //【5】Merge original and compressed tracks
    val result = (targetTrack ++ track2SpaceTime ++ moveOutTrack).toArray

    //【6】Generate the trajectory scatter plot
    if(generateOrigView){
      val origTrackPath = s"${viewFilePath}${File.separator}${orgTrackFileName}.html"
      createFile(new File(origTrackPath))

      TrackView.replaceCoordinates(viewHtmlTemplate.getAbsolutePath
        , newCoordinates = targetTrack.map(r => new LatLng(r.latitude, r.longitude)).toArray
        , origTrackPath )
    }

    if(generateQuantiView){
      val quantiTrackPath = s"${viewFilePath}${File.separator}${quantiTrackFileName}.html"
      createFile(new File(quantiTrackPath))

      TrackView.replaceCoordinates(viewHtmlTemplate.getAbsolutePath
        , newCoordinates = track2SpaceTime.map(r => new LatLng(r.latitude, r.longitude))
        , quantiTrackPath )
    }

    if(metricInfo != null ) metricInfo.append((targetTrack.size, track2SpaceTime.size, similar, travelModes))
  }


  /**
    * Create the specified file, and if it does not exist, create the corresponding parent directory.
    * @param file
    */
  private def createFile(file: File): Unit = {
      //If the file path does not exist, create the path.
      if(!file.getParentFile.exists()){
        if(!file.getParentFile.getParentFile.exists())
          file.getParentFile.getParentFile.mkdir()
        file.getParentFile.mkdir()
      }

  }

  /**
    * Latitude and Longitude Standard Conversion
    */
  private def convertTest(): Unit = {
    val wgsLongitude = 116.370743 // WGS84经度
    val wgsLatitude = 23.5388 // WGS84纬度
    val baiduPoint = TrackCoordinateConverter.gps84_To_bd09(wgsLatitude, wgsLongitude)

    println("Baidu Map coordinates (BD-09):")
    println("Longitude: " + baiduPoint(1))
    println("Latitude: " + baiduPoint(0))
  }


  private def test(): Unit = {
    val points = Array(
      ("13312135675", 28182789, 23.221857, 113.31165),
      ("13312135675", 28182789, 23.22186, 113.311646)
    )
    //val result = PointService.filterOutliers(points.map(point => LatLng(point._3, point._4)))
    //println(result.size)
  }





}