package com.track.mining.database

import java.sql.{Connection, DriverManager}

import com.track.mining.common.{Logging, TrackCoordinateConverter}
import com.track.mining.trackmanager.bean.Point

import scala.collection.mutable.ArrayBuffer

/**
  * Trajectory Data Persistence and Database Insertion Class
  */
class MysqlDBManager extends DBManager with BaseManager {

  val url = DatabaseConfig.getUrl
  val username = DatabaseConfig.getUsername
  val password = DatabaseConfig.getPassword
  Class.forName(DatabaseConfig.getDriver)

  /**
    * Batch import trajectory data into the database
    * @param TrackData
    */
  override def batchInsertData(TrackData:ArrayBuffer[Point]): Unit = {
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(url, username, password) // 建立与MySQL数据库的连接
      super.batchInsertData(TrackData, conn)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.rollback() // 如果发生错误则回滚事务
        conn.close() // 关闭数据库连接
      }
    }
  }

  /**
    * Load raw trajectory data from the database
    * @return return the result
    */
  override def loadEventData(queryCon:String, dateCon:String, filterTag:String): ArrayBuffer[Point] = {
    var conn: Connection = null
    val result = new ArrayBuffer[Point]
    try {
      conn = DriverManager.getConnection(url, username, password) // 建立与MySQL数据库的连接
      super.loadEventData(conn, queryCon, dateCon, filterTag)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        result
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close() // 关闭数据库连接
      }
    }
  }

  /**
    * Retrieve device information from the data collection device within the trajectory data
    * @return Returns the device information
    */
  override def getTotalDeviceID(): ArrayBuffer[String] ={
    var conn: Connection = null
    val deviceIDs = new ArrayBuffer[String]
    try {
      conn = DriverManager.getConnection(url, username, password) // 建立与MySQL数据库的连接
      super.getTotalDeviceID(conn)
    }catch {
      case e:Exception  =>
        e.printStackTrace()
        deviceIDs
    }finally {
      if (conn != null && !conn.isClosed()) {
        conn.close() // 关闭数据库连接
      }
    }
  }


  /**
    * Retrieve the dates contained within the track data
    * @return An array of dates
    */
  override def getTotalTrackDate(): ArrayBuffer[String] ={
    var conn: Connection = null
    val trackDate = new ArrayBuffer[String]
    try {
      conn = DriverManager.getConnection(url, username, password) // 建立与MySQL数据库的连接
      super.getTotalTrackDate(conn)
    }catch {
      case e:Exception  =>
        e.printStackTrace()
        trackDate
    }finally {
      if (conn != null && !conn.isClosed()) {
        conn.close() // 关闭数据库连接
      }
    }
  }

  /**
    * Load raw trajectory data from the database
    * @return Returns trajectory information
    */
  def loadOldEventData(): ArrayBuffer[Point] = {
    var conn: Connection = null
    val result = new ArrayBuffer[Point]
    try {
      conn = DriverManager.getConnection(url, username, password) // 建立与MySQL数据库的连接
      val querySQL = """select device_id, latitude, longitude, altitude, speed, data_type, date from TrackMananger.tb_s_event_0229 where device_id<>'13567532345' """.stripMargin // SQL语句模板
      val st = conn.createStatement()
      val rs = st.executeQuery(querySQL)
      while (rs.next()){
        val wgsLatitude = rs.getFloat(2)
        val wgsLongitude = rs.getFloat(3)
        val baiduPoint = TrackCoordinateConverter.gps84_To_bd09(wgsLatitude,wgsLongitude)

        result.append(
          Point(rs.getString(1), baiduPoint(0).toFloat, baiduPoint(1).toFloat,
            rs.getFloat(4), rs.getFloat(5), rs.getInt(6),
            rs.getString(7)) )
      }
      result
    } catch {
      case e: Exception =>
        e.printStackTrace()
        result
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close() // 关闭数据库连接
      }
    }
  }

  override def initTable(): Unit = {
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(url, username, password) // 建立与MySQL数据库的连接
      super.initTable(conn)
    }catch {
      case e:Exception  =>
        e.printStackTrace()
    }finally {
      if (conn != null && !conn.isClosed()) {
        conn.rollback() // 如果发生错误则回滚事务
        conn.close() // 关闭数据库连接
      }
    }
  }

  override def clearEventData(tableName:String): Unit = {
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(url, username, password) // 建立与MySQL数据库的连接
      super.clearEventData(conn, tableName)
    }catch {
      case e:Exception  =>
        e.printStackTrace()
    }finally {
      if (conn != null && !conn.isClosed()) {
        conn.rollback() // 如果发生错误则回滚事务
        conn.close() // 关闭数据库连接
      }
    }
  }

  override def insertCompressPoint(TrackData: Array[Point],isTruncate:Boolean): Boolean = {
    false
  }
}
