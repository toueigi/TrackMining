package com.track.mining.database

object DBTest {

  val dbType = 1 //0:mysql;  1:duckdb
  val baseManager: BaseManager = dbType match {
    case 0 => new MysqlDBManager()
    case 1 => new DuckDBManager()
    case _ => new MysqlDBManager()
  }

  def main(args: Array[String]): Unit = {
    baseManager.clearEventData("tb_trk_compress_track")
  }
}
