package com.track.mining.trackmanager

import java.io.{File, FileInputStream, InputStream}
import java.text.SimpleDateFormat

import com.track.mining.common.{Logging, TrackCoordinateConverter}
import com.track.mining.database.{BaseManager, DuckDBManager, MysqlDBManager}
import com.track.mining.trackmanager.bean.Point
import com.track.mining.trackmanager.service.{MysqlTrackService, PointService}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Trajectory Data Preprocessing Workflow: This workflow performs preprocessing, coordinate system conversion,
  * database ingestion, and other related operations on collected trajectory data.
  */
object TrackDataProcess extends Logging{

  val dbType = 0 //0:mysql;1:duckdb
  val baseManager: BaseManager = dbType match {
    case 0 => new MysqlDBManager()
    case 1 => new DuckDBManager()
    case _ => new MysqlDBManager()
  }
  def main(args: Array[String]): Unit = {
    this.readDataFromPath("data\\track\\geolife")
  }

  /**
    * Track data existing in Excel files will undergo standardized processing before being imported into the designated database.
    */
  def readDataFromPath(filePath:String = "data\\track"):Unit = {
    val sourceFile: File = new File(filePath)
    val totalDevices = new ArrayBuffer[String] //baseManager.getTotalDeviceID()
    println(s"The number of device IDs retrieved is :${totalDevices.size}")
    parDataFile(sourceFile, totalDevices)
  }

  /**
    * Recursively read data from the specified path and parse the data.
    * @param soureFilePath Specified data storage directory
    * @param totalDevices Analyze the corresponding data acquisition device information in the data.
    */
  def parDataFile(soureFilePath:File, totalDevices:ArrayBuffer[String]):Unit = {
    for (sourceFilePath <- soureFilePath.listFiles()) {
      val filename = sourceFilePath.getName
      if(sourceFilePath.isDirectory){
        parDataFile(sourceFilePath, totalDevices)
      }
      else if (sourceFilePath.isFile && !totalDevices.contains(filename.substring(0, filename.lastIndexOf(".")))
        && !filename.startsWith("~$") && filename.endsWith("csv") && sourceFilePath.getAbsolutePath.contains("volunteers")) {
        convertExcelInfo(sourceFilePath)
      }
      else if (sourceFilePath.isFile && filename.endsWith("plt") && sourceFilePath.getAbsolutePath.contains("geolife")) {
        parseGeoLifeData(sourceFilePath)
      }
    }
  }

  /**
    *
    * @param sourceFilePath Read Excel files and insert them into the database.
    *
    */
  def convertExcelInfo(sourceFilePath:File) = {
    val lines = Source.fromFile(sourceFilePath,"utf-8").getLines()
    val points = new  ArrayBuffer[Point]
    val deviceID = sourceFilePath.getName.substring(0, sourceFilePath.getName.lastIndexOf("."))
    var count  = 0
    try {
      for (line <-  lines) {
        if(count >0 ){
          val info  = line.split(",")
          val geoTime              = info(0).trim.toLong
          val wgsLatitude             = info(1).trim.toDouble
          val wgsLongitude            = info(2).trim.toDouble
          val altitude             = info(3).trim.toFloat
          val course               = info(4).trim
          val horizontalAccuracy   = info(5).trim
          val verticalAccuracy     = info(6).trim
          val speed                = info(7).trim.toFloat
          val status               = info(8).trim
          val activity             = info(9).trim
          val network              = info(10).trim
          val appStatus            = info(11).trim
          val dayTime              = info(12).trim
          val formattedDateTime :String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(geoTime)
          //Convert GPS coordinates to Baidu coordinates
          //val baiduPoint = if(dbType == 0) TrackCoordinateConverter.gps84_To_bd09(wgsLatitude,wgsLongitude) else Array(wgsLatitude,wgsLongitude)
          val baiduPoint =  Array(wgsLatitude,wgsLongitude)
          val event = if(geoTime>0) new Point(deviceID, baiduPoint(0), baiduPoint(1), altitude, speed, 0, formattedDateTime) else null
          points.append(event)
        }
        count = count +1
      }
      logInfo(s"#####################start import data, data filename ：${deviceID}")
      baseManager.batchInsertData(points)
      logInfo(s"#####################data import finish! data count : ${points.size}")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logError(s"Parsing document ${deviceID} error: Data read error on line ${count}！")
    }
  }

  /**
    *
    * @param sourceFilePath Read the GeoLifeData data file and insert it into the database.
    *
    */
  def parseGeoLifeData(sourceFilePath:File) = {
    val lines = Source.fromFile(sourceFilePath,"utf-8").getLines()
    val points = new  ArrayBuffer[Point]
    val deviceID = sourceFilePath.getParentFile.getParentFile.getName
    var count  = 0
    try {
      for (line <-  lines) {
        val info  = line.split(",")
        if(info.size == 7){
          val wgsLatitude          = info(0).trim.toDouble
          val wgsLongitude         = info(1).trim.toDouble
          val altitude             = info(3).trim.toFloat
          val formattedDateTime    = info(5).trim + " " +info(6).trim
          val dateInfo = MysqlTrackService.dateToTimestamp(formattedDateTime)
          val speed = 0

          val baiduPoint =  Array(wgsLatitude,wgsLongitude)
          val event = if(dateInfo._2 > 0) new Point(deviceID, baiduPoint(0), baiduPoint(1), altitude, speed, 0, formattedDateTime) else null
          points.append(event)
        }
        count = count +1
      }
      logInfo(s"#####################start import data, data filename ：${deviceID}")
      baseManager.batchInsertData(points)
      logInfo(s"#####################data import finish! data count : ${points.size}")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logError(s"Parsing document ${deviceID} error: Data read error on line ${count}！")
    }
  }


  /**
    * Generate virtual device counts and trajectory information for a specified time interval
    * @param startDate Start date in yyyyMMdd format
    * @param endDate End date in yyyyMMdd format
    * @param target_cnt Target device ID count
    */
  def generateTrackData(startDate:Int, endDate:Int, target_cnt:Int)={
    for(date <- (startDate until endDate)){
      val generateData = new ArrayBuffer[Point]()
      for(cnt <- (0 until target_cnt)){
        (1 to 10).foreach { i =>
          val trajectoryData = PointService.generateTrajectoryData(s"test_${cnt}", date.toString)
          generateData.append(trajectoryData)
        }
      }
      baseManager.batchInsertData(generateData)
      generateData.clear()
    }
  }

  /**
    *
    * Generate test data in bulk for comparative testing.
    */
  def productTrackData = {
    generateTrackData( 20230720,20230725, 100000)
  }

//  def batchUpdatePosition():Unit = {
//    baseManager.loadEventData("","")
//      .groupBy(_.device_id)
//      .map(row => {
//        baseManager.batchInsertData(row._2)
//      })
//  }
}
