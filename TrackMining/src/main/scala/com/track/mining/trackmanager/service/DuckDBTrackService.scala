package com.track.mining.trackmanager.service

import com.track.mining.database.{BaseManager, DuckDBManager, MysqlDBManager}
import com.track.mining.trackmanager.bean.Point
import com.track.mining.trackmanager.service.MysqlTrackService.baseManager

object DuckDBTrackService {
  val level: Int = 15
  val dbType = 1 //0：mysql;1：duckdb
  val baseManager: BaseManager = dbType match {
    case 0 => new MysqlDBManager()
    case 1 => new DuckDBManager()
    case _ => new MysqlDBManager()
  }


  def insertTrackToDuckDB(tracks: Array[Point], isTruncate:Boolean  = false):Boolean = {
    baseManager.insertCompressPoint(tracks, isTruncate)
  }
}
