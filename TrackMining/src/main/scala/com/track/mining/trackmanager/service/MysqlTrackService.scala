package com.track.mining.trackmanager.service

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}

import com.google.common.geometry.{S2CellId, S2LatLng, S2Point}
import com.track.mining.common.TrackCoordinateConverter
import com.track.mining.database.{BaseManager, DuckDBManager, MysqlDBManager}
import com.track.mining.trackmanager.bean.Point

import scala.collection.mutable.ArrayBuffer

/**
  * About mysql database functions
  */

object MysqlTrackService {
  val level: Int = 15
  val dbType = 0 //0：mysql;1：duckdb
  val baseManager: BaseManager = dbType match {
    case 0 => new MysqlDBManager()
    case 1 => new DuckDBManager()
    case _ => new MysqlDBManager()
  }

  /**
    * query the track from database
    * @param queryTarget
    * @param dateCondition filter condition
    * @param isConvert if convert the Coordinate
    * @param filterTag if query the simulated data
    * @return track info
    */
  def dataConvertSpaceTime(queryTarget:String ="" ,dateCondition:String = "", isConvert:Boolean = false,
                           filterTag:String = "") =
    baseManager.loadEventData(queryTarget, dateCondition, filterTag)
    .map(row => {
      val dateInfo = dateToTimestamp(row.date)
      val gcj02Point = TrackCoordinateConverter.bd09_To_Gcj02(row.latitude, row.longitude)
      val point =  if(isConvert) (gcj02Point(0).toString.toDouble , gcj02Point(1).toString.toDouble ) else (row.latitude, row.longitude)
      new Point(device_id = row.device_id, latitude = point._1, longitude = point._2, row.altitude, row.speed, row.data_type, row.date,
        capturetime = dateInfo._2)
    })

  /**
    * static the total dates of the data
    * @return
    */
  def getTotalTrackDate(): Array[String] = {
    baseManager.getTotalTrackDate.sorted.sliding(2,1).map(row => row.mkString(",")).toArray
  }

  /**
    * convert date to timstamp
    * @param dateStr
    * @param format
    * @return timstamp
    */
  def dateToTimestamp(dateStr: String, format: String = "yyyy-MM-dd hh:mm:ss"): (java.util.Date, Long) = {
    val formatter = new SimpleDateFormat(format)
    val date = formatter.parse(dateStr)
    (date , date.getTime / 1000)
  }

  /**
    * Format date to yyyy-MM-dd
    * @param dateStr
    * @param format
    * @return (date , timestamp)
    */
  def getDayTimestampFromDate(dateStr: String, format: String = "yyyy-MM-dd"): (java.util.Date, Long) = {
    val formatter = new SimpleDateFormat(format)
    val date = formatter.parse(dateStr)
    (date , date.getTime / 1000)
  }


  /**
    * return the date from timestamp
    * @param timestamp
    * @return date (yyyy-MM-dd HH:mm:ss)
    */
  def getDateFromTimeStamp(timestamp:Long):String = {
    // 将时间戳转换为Instant对象
    val instant: Instant = Instant.ofEpochSecond(timestamp)
    // 将Instant转换为ZonedDateTime对象，这里使用的是系统默认时区
    // 如果你需要特定的时区，可以替换ZoneId.systemDefault()为ZoneId.of("你的时区ID")
    val zonedDateTime: ZonedDateTime = instant.atZone(ZoneId.systemDefault())
    // 创建一个DateTimeFormatter来定义日期格式
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    // 使用formatter格式化日期
    val formattedDate: String = zonedDateTime.format(formatter)
    formattedDate
  }




}
