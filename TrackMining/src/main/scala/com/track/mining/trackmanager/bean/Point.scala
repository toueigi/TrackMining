package com.track.mining.trackmanager.bean

case class Point(device_id:String, latitude:Double, longitude:Double, altitude:Float = 0.0F, speed:Float= 0.0F,
                   data_type:Int, date:String, s2cell:Long =0, capturetime:Long =0, timespan:Long = 0)

case class StayPoint(device_id:String, begin_time:Int, end_time:Int, latitude:Double,
                       longitude:Double, altitude:Float, timespan:Float, date:String)


case class LatLng(latitude: Double, longitude: Double) extends Serializable