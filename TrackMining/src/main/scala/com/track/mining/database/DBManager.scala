package com.track.mining.database

import java.sql.{Connection, DriverManager, Statement}

import com.track.mining.common.Logging
import com.track.mining.trackmanager.bean.Point

import scala.collection.mutable.ArrayBuffer

abstract class DBManager extends Logging{

  def getTotalDeviceID(conn:Connection): ArrayBuffer[String] ={

    val deviceIDs = new ArrayBuffer[String]
    try {
      val querySQL = """select distinct device_id from tb_s_event_wgs """.stripMargin // SQL语句模板
      val st = conn.createStatement()
      val rs = st.executeQuery(querySQL)
      while (rs.next()){
        deviceIDs.append(rs.getString(1))
      }
      deviceIDs
    }catch {
      case e:Exception  =>
        e.printStackTrace()
        deviceIDs
    }
  }

  def getTotalTrackDate(conn:Connection): ArrayBuffer[String] ={

    val days = new ArrayBuffer[String]
    var querySQL = ""
    try {
      querySQL = """select substr(date,1,10) as date from tb_s_event_wgs group by substr(date,1,10) order by substr(date,1,10)""".stripMargin // SQL语句模板
      val st = conn.createStatement()
      val rs = st.executeQuery(querySQL)
      while (rs.next()){
        days.append(rs.getString(1).substring(0,10))
      }
      days
    }catch {
      case e:Exception  =>
        e.printStackTrace()
        println(s"Execute SQL statements：${querySQL} error！！")
        days
    }
  }

  def batchInsertData(TrackData:ArrayBuffer[Point], conn:Connection): Unit = {
    try {
      conn.setAutoCommit(false) // 关闭自动提交事务
      val sql =
        """INSERT INTO tb_s_event_wgs_geolife (device_id, latitude, longitude, altitude, speed, data_type, date)
           VALUES (?, ?, ?, ?, ?, ?, ?)""".stripMargin // SQL语句模板
      for (i <- 1 to TrackData.size) { // 生成要插入的数据（这里只作为示例）
        val point = TrackData(i-1)
        // 设置参数值
        val preparedStatement = conn.prepareStatement(sql)
        preparedStatement.setString(1, point.device_id)
        preparedStatement.setDouble(2,  point.latitude)
        preparedStatement.setDouble(3,  point.longitude)
        preparedStatement.setFloat(4,  point.altitude)
        preparedStatement.setFloat(5,  point.speed)
        preparedStatement.setInt(6,    point.data_type)
        preparedStatement.setString(7, point.date)
        // 执行插入操作
        preparedStatement.executeUpdate()
      }
      conn.commit() // 提交事务
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def loadEventData(conn: Connection, queryCon:String = "", dateCon:String="", filterTag:String = ""): ArrayBuffer[Point] = {
    val result = new ArrayBuffer[Point]
    try {
      var querySQL = ""
        if(filterTag == null || "".equals(filterTag)){
          querySQL ="""select device_id, latitude, longitude, altitude, speed, data_type, date
                          from TrackMananger.tb_s_event_wgs where 1=1 """.stripMargin // SQL语句模板
        }
        else{
          val targetCond = filterTag.split(",")
          querySQL = s"""
              select device_id, latitude, longitude, altitude, speed, data_type, date
              from TrackMananger.tb_s_event_wgs
              where 1=1 and (
              (cast(substr(device_id, 6)  AS SIGNED) between ${targetCond(0)} and ${targetCond(1)})
              or (device_id REGEXP '^[0-9]+')
              )"""
        }

//      var querySQL =
//        """select device_id, latitude, longitude, altitude, speed, data_type, date
//          from TrackMananger.tb_s_event_wgs_test where 1=1 """.stripMargin // SQL语句模板
      if(queryCon.nonEmpty ){
        val condition = queryCon.split(",").mkString("','")
        querySQL = s" ${querySQL} and  device_id in ('${condition}') "
      }
      if(dateCon.nonEmpty && dateCon.split(",").size >0 ){
        val condition = dateCon.split(",")
        querySQL = s" ${querySQL} and date between '${condition(0)}' and '${condition(1)}'"
      }
      println(s"Query statement：${querySQL}")
      val st = conn.createStatement()
      val rs = st.executeQuery(querySQL)
      while (rs.next()){
        result.append(
          Point(
            rs.getString(1),
            rs.getString(2).toDouble,
            rs.getString(3).toDouble,
            rs.getFloat(4),
            rs.getFloat(5),
            rs.getInt(6),
            rs.getString(7))
        )
      }
      result.filter(r => r.latitude >0.0 && r.longitude >0.0)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        println(s"Analyzing Date Type Errors，${dateCon}")
        result
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close() // 关闭数据库连接
      }
    }
  }


  def initTable(conn:Connection):Unit = {
    val stmt: Statement = null
    try {
      // create a table
      val stmt = conn.createStatement
      val trkCompressTbl =
        s" CREATE TABLE IF NOT EXISTS tb_trk_compress_track(" +
        s" device_id VARCHAR, " +
        s" capturetime INT64, " +
        s" latitude DOUBLE," +
        s" longitude DOUBLE," +
        s" s2cell VARCHAR," +
        s" pointtype INT )"
      logInfo(s"Ceate table： ${trkCompressTbl}")
      stmt.execute(trkCompressTbl)

      val eventTbl = s"CREATE TABLE IF NOT EXISTS tb_s_event_wgs(" +
        s" id BIGINT," +
        s" device_id VARCHAR NOT NULL ," +
        s" latitude DOUBLE DEFAULT NULL ," +
        s" longitude DOUBLE DEFAULT NULL," +
        s" altitude DOUBLE DEFAULT NULL," +
        s" speed DOUBLE DEFAULT NULL," +
        s" data_type INT DEFAULT NULL," +
        s" date datetime DEFAULT NULL," +
        s" pointtype INT )"
      logInfo(s"Ceate table：${eventTbl}")
      stmt.execute(eventTbl)

      val userTbl = s" CREATE TABLE IF NOT EXISTS  tb_user (" +
        s" id BIGINT, " +
        s" name VARCHAR(64) DEFAULT NULL," +
        s" sex VARCHAR(1) DEFAULT '0' ," +
        s" phone_number VARCHAR(11) DEFAULT NULL ," +
        s" address VARCHAR(128) DEFAULT NULL," +
        s" id_card VARCHAR(18) DEFAULT NULL," +
        s" emergency_contact_name VARCHAR(64) DEFAULT NULL," +
        s" emergency_contact_phone VARCHAR(11) DEFAULT NULL," +
        s" create_time datetime DEFAULT NULL ," +
        s" last_update_time datetime DEFAULT NULL," +
        s" create_user VARCHAR(64) DEFAULT NULL," +
        s"  last_update_user VARCHAR(64) DEFAULT NULL," +
        s" state VARCHAR(1) DEFAULT '1' ," +
        s" longitude DOUBLE DEFAULT NULL," +
        s" latitude DOUBLE DEFAULT NULL)"

      logInfo(s"Ceate table：${userTbl}")
      stmt.execute(userTbl)


    } catch {
      case e: Exception =>
        logError("Failed to create related tables during database initialization！！！")
        e.printStackTrace()
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close() // 关闭数据库连接
      }
    }
  }

  def clearEventData(conn:Connection, tableName:String ) :Unit = {
    val stmt: Statement = null
    try {
      // create a table
      val stmt = conn.createStatement
      val trkCompressTbl = s"TRUNCATE ${tableName}"
      logInfo(s"delete the data of tb_s_event_wgs：${trkCompressTbl}")
      stmt.execute(trkCompressTbl)
    } catch {
      case e: Exception =>
        logError("Data truncate failed！！！")
        e.printStackTrace()
    } finally {
      if (conn != null && !conn.isClosed()) {
        conn.close() // 关闭数据库连接
      }
    }
  }

}
