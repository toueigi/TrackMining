package com.track.mining.database

import java.sql.{Connection, DriverManager, Statement}

import com.track.mining.common.Logging
import com.track.mining.trackmanager.bean.Point

import scala.collection.mutable.ArrayBuffer

class DuckDBManager extends DBManager with BaseManager{

  val databasePath = "D:/zhangws/duckdb/trakmanager.duckdb"
  Class.forName("org.duckdb.DuckDBDriver")
  val conn = DriverManager.getConnection(s"jdbc:duckdb:${databasePath}")

  override def batchInsertData(TrackData: ArrayBuffer[Point]): Unit = {
    var stmt: Statement = null
    try {
      super.batchInsertData(TrackData, conn)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  override def loadEventData(queryCon: String, dateCon: String, filterTag:String): ArrayBuffer[Point] = {
    val result = new ArrayBuffer[Point]
    try {
      super.loadEventData(conn, queryCon, dateCon)
      result
    } catch {
      case e: Exception =>
        e.printStackTrace()
        result
    }
  }

  override def getTotalDeviceID(): ArrayBuffer[String] = {
    var conn: Connection = null
    val deviceIDs = new ArrayBuffer[String]
    try {
      conn = DriverManager.getConnection(s"jdbc:duckdb:${databasePath}")
      super.getTotalDeviceID(conn)
    }catch {
      case e:Exception  =>
        e.printStackTrace()
        deviceIDs
    }
  }

  override def getTotalTrackDate(): ArrayBuffer[String] = {
    return null
  }

  @throws[Exception]
  def insertCompressPoint(TrackData:Array[Point], isTruncate:Boolean): Boolean = {
    var flag = false
    val stmt: Statement = null
    try{
      // create a table
      val stmt = conn.createStatement
      val createTbl = s"CREATE TABLE IF NOT EXISTS tb_trk_compress_track(" +
        s" device_id VARCHAR, " +
        s" capturetime INT64, " +
        s" latitude DOUBLE," +
        s" longitude DOUBLE," +
        s" s2cell INT64," +
        s" pointtype INT" +
        s")"
      stmt.execute(createTbl)
      logInfo(s"执行创建表语句：${createTbl}")

      val trkCompressTbl = s"TRUNCATE tb_trk_compress_track"
      stmt.execute(trkCompressTbl)
      logInfo(s"truncate table tb_trk_compress_track：${trkCompressTbl}")

      conn.setAutoCommit(false)
      val insertSql = "INSERT INTO tb_trk_compress_track VALUES (?, ?, ?, ?, ?, ?)"
      for (i <- 1 to TrackData.size) { // 生成要插入的数据（这里只作为示例）
        val point = TrackData(i-1)
        // 设置参数值
        val preparedStatement = conn.prepareStatement(insertSql)
        preparedStatement.setString(1, point.device_id)
        preparedStatement.setLong(2,  point.capturetime)
        preparedStatement.setDouble(3,  point.latitude)
        preparedStatement.setDouble(4,  point.longitude)
        preparedStatement.setLong(5,  point.s2cell)
        preparedStatement.setInt(6,    point.data_type)
        // 执行插入操作
        preparedStatement.executeUpdate()
      }
      conn.commit() // 提交事务
      logInfo("Completed importing trajectory data into the database！")
      flag = true
      flag
    }catch {
      case e: Exception =>
        logError(s"Trajectory data import into DUCKDB encountered an error！")
        e.printStackTrace()
        flag
    }
  }

  override def initTable(): Unit = {
    try {
      super.initTable(conn)
    }catch {
      case e:Exception  =>
        e.printStackTrace()
    }
  }

  override def clearEventData(tableName:String) :Unit = {
    try {
      super.clearEventData(conn, tableName)
    }catch {
      case e:Exception  =>
        e.printStackTrace()
    }
  }
}
