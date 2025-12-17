package com.track.mining.database

import java.sql.{Connection, DriverManager, Statement}

import com.track.mining.trackmanager.bean.Point

import scala.collection.mutable.ArrayBuffer

trait BaseManager {

  def batchInsertData(TrackData: ArrayBuffer[Point]): Unit

  def loadEventData(queryCon: String, dateCon: String, filterTag:String): ArrayBuffer[Point]

  def getTotalDeviceID(): ArrayBuffer[String]

  def initTable() :Unit

  def clearEventData(tableName:String) :Unit

  def insertCompressPoint(TrackData: Array[Point], isTruncate:Boolean):Boolean

  def getTotalTrackDate(): ArrayBuffer[String]
}
