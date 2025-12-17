package com.track.mining.trackmanager

import java.io.File

import com.google.common.geometry.{MyS2Encode, S2CellId, S2LatLng}
import com.track.mining.common.Logging
import com.track.mining.database.{BaseManager, DuckDBManager, MysqlDBManager}
import com.track.mining.trackmanager.bean.{LatLng, Point}
import com.track.mining.trackmanager.service.{MysqlTrackService, PointService, TrackSimilar}
import org.apache.spark.util.SizeEstimator

import scala.collection.mutable.ArrayBuffer

/**
  * This category pertains to statistical indicators related to Experiment 2.
  *
  */
object TrackDataSimilar extends Logging {


  val outFile: File = new File("data\\output\\chapter5.3\\tables")
  val s2_level = 16
  val dbType = 0 //0:mysql;1:duckdb
  val baseManager: BaseManager = dbType match {
    case 0 => new MysqlDBManager()
    case 1 => new DuckDBManager()
    case _ => new MysqlDBManager()
  }

  def main(args: Array[String]): Unit = {
    this.AllTrackProductQuantization()
  }

  /**
    * Perform product quantization compression on all date-based data
    * Query in batches by date
    * Subsequent comparisons can be made using different dates to evaluate compression effectiveness
    */
  def AllTrackProductQuantization(): Unit = {

    val statTime = System.currentTimeMillis()
    log.info(s"Begin calculating similarity and storage compression ratio!!!")
    //(device_id, date, st_track_similar, vector_track_similar, org_track_storage, st_track_storage, vector_track_storage)
    //new ArrayBuffer[(String, String, Double, Double, Long, Long, Long) ]
    //【1】load data from database
    // return：Array[(phonenum, capturetime,lngitude,latitude)]
    val result = MysqlTrackService
      .dataConvertSpaceTime("", "",false, filterTag = "0,0")
      .map(row => {
          ((row.device_id, row.date.substring(0, 10)) ,row)
      })
      .groupBy(_._1)
      .map(trackInfo => TrackProductQuantization(trackInfo._1._1, trackInfo._1._2, trackInfo._2.map(_._2)) )

    log.info(s"Completion of similarity and storage evaluation for all trajectories, with a total duration of ${(System.currentTimeMillis() - statTime)/1000} seconds")
    //【2】Output the result
    val (storageComp, compAndPreci) = TrackSimilar.summarizeByPeriod(result.toArray)
    DataWarehouse.saveAsTxtFile(outFile + File.separator + "Table9_Storage_Compression_Analysis_Estimate.txt", storageComp)
    DataWarehouse.saveAsTxtFile(outFile + File.separator + "Table10_Compression_and_Precision_Analysis_Estimate.txt", compAndPreci)

  }


  /**
    * Comparison of pre- and post-quantization effects for temporal trajectory multiplication
    * * Iterate through each target number to retrieve daily travel trajectories
    *
    * * Output: device_id, date, similarity between quantized multiplication trajectory and original trajectory
    * , similarity between quantized trajectory and original trajectory
    * , original trajectory storage, quantized multiplication trajectory storage, vectorized trajectory storage
    *
    * @param queryTarget
    * @param beginDate
    * @param targetTrack
    * @return (queryTarget, queryDate, st_track_similar, vector_track_similar, org_track_storage, st_track_storage, vector_track_storage)
    */
  def TrackProductQuantization(queryTarget:String, beginDate:String, targetTrack: ArrayBuffer[Point])
  : (String, String, Double, Double, Long, Long, Long) = {

    val wholeDay = MysqlTrackService.getDayTimestampFromDate(beginDate.substring(0, 10))
    if(targetTrack != null && targetTrack.size > 1){
      //【1】Outlier Removal Based on DBSCAN
      val removeOutlierTrack = PointService.removeOutlierInTrajectory(targetTrack.toArray)
        .sortBy(_.capturetime)
      //【2】Using double variance to further eliminate outliers
      val filterTrack = removeOutlierTrack
        .map(row => {
          ((row.device_id, (row.capturetime - wholeDay._2) / 120), row)
        })
        .groupBy(_._1)
        .mapValues(points => {
          PointService.filterOutliers(points.map(_._2))
        }).flatMap(row => row._2)
        .toArray
        .sortBy(_.capturetime)

      //【3】Perform product quantization on the trajectory data
      //  return：List[((phonenum,timespan),Array((lngitude,latitude), S2CellId))]
      val track2SpaceTime = PointService.removeOutlierInTrajectory(targetTrack.toArray)
        .map(row => {
          val wholeDay = MysqlTrackService.getDayTimestampFromDate(row.date.substring(0, 10))
          ((row.device_id, (row.capturetime - wholeDay._2) / 120), row)
        }).sortBy(_._1._2)
        .groupBy(_._1)
        .mapValues(points => {
          val filterPoints = PointService.filterOutliers(points.map(_._2)).sortBy(_.capturetime)
          if (filterPoints.isEmpty) {
            logError("Trajectory filtering error！")
            //points.map(row => s"${row._1._1}, ${row._1._2}, ${row._2.latitude}，${row._2.longitude}").map(print(_))
          }
          //Utilize clustering algorithms to group the trajectories within this time slice.
          PointService.pointCluster(filterPoints, 50)
        })
        .flatMap(tracks => {
          tracks._2.map(point => {
            new Point(device_id = tracks._1._1, latitude = point._1, longitude = point._2, 0.0f, 0.0f, 1, "",
              capturetime = point._3, timespan = tracks._1._2)
          })
        })
        .toArray
        .sortBy(_.capturetime)

      //【4】Further vectorize the product-quantized trajectory
      val encodeTrack = track2SpaceTime.map(row => {
        // 【1.1】Create S2LatLng, convert latitude and longitude to radians to calculate spherical coordinates
        val latLng = S2LatLng.fromDegrees(row.latitude, row.longitude)
        //【1.2】Create custom S2 spatiotemporal encoding
        val myS2Encode = MyS2Encode.fromLatLng(row.timespan, latLng).parent(s2_level)
        (row.device_id, myS2Encode.id())
      })

      //Storage capacity of the original trajectory
      val org_track_storage = SizeEstimator.estimate(targetTrack.map(row => (row.capturetime,row.latitude,row.longitude,row.device_id)))
      val st_track_storage = SizeEstimator.estimate(track2SpaceTime.map(row => (row.capturetime,row.latitude,row.longitude,row.device_id)))
      val vector_track_storage = SizeEstimator.estimate(encodeTrack)

      val decodeTrack = encodeTrack.map(r => {
        decodeMyS2Encode(r._1, r._2, wholeDay._2)
      }).sortBy(_.capturetime)

      val st_track_similar     = TrackSimilar.DTW( filterTrack, track2SpaceTime, 120, gridLevel = s2_level)
      val vector_track_similar = TrackSimilar.DTW( filterTrack, decodeTrack, 120, gridLevel = s2_level)
      //println(s"The similarity before and after trajectory compression is：${final_track_similar}, Storage is：${final_track_storage}")
      (queryTarget, beginDate, st_track_similar, vector_track_similar, org_track_storage, st_track_storage, vector_track_storage)
    }else{
      ("", "", 0.0, 0.0, 0, 0, 0)
    }
  }

  /**
    * Decode the custom S2 encoding and output the Point.
    * @param deviceId
    * @param myS2EncodeId Custom S2 Encoding
    * @param wholeDayCaptureTime The timestamp at 00:00 on the current date
    * @return
    */
  def decodeMyS2Encode(deviceId:String, myS2EncodeId:Long, wholeDayCaptureTime:Long):Point={
    val s2cellInfo = new MyS2Encode(myS2EncodeId).decodeFromMyS2EncodeId()
    val timespan = s2cellInfo.split("\\|")(0)
    val s2CellId = new S2CellId(s2cellInfo.split("\\|")(1).toLong)
    val s2LatLng: S2LatLng  = s2CellId.toLatLng()
    new Point(device_id = deviceId, latitude = s2LatLng.latDegrees(), longitude = s2LatLng.lngDegrees(),
      capturetime = wholeDayCaptureTime + timespan.toLong * 120, data_type=2 ,date = "")
  }


  /**
    * A point cluster test method
    */
  def testCluster() = {
    val tracks = Array(
      ("18923440909","2023-07-04 19:57:07",23.3967826,113.46415649),
      ("18923440909","2023-07-04 19:58:38",23.3967770,113.46419673),
      ("18923440909","2023-07-04 19:58:48",23.396577,113.464189),
      ("18923440909","2023-07-04 19:59:08",23.397729,113.462625),
      ("15676523452","2023-07-04 19:57:07",23.3967826,113.46415649),
      ("15676523452","2023-07-04 19:58:38",23.3967770,113.46419673),
      ("15676523452","2023-07-04 19:58:48",23.396577,113.464189),
      ("15676523452","2023-07-04 19:59:08",23.397729,113.462625),
      ("15676523452","2023-07-04 19:59:45",23.397749,113.462635))


    val track2ST = tracks.map(r => {
      val capture_time = MysqlTrackService.dateToTimestamp(r._2)
      new Point(device_id = r._1, latitude = r._3, longitude = r._4, capturetime = capture_time._2, data_type=0 ,date = r._2)
    })
      .map(row => {
        val wholeDay = MysqlTrackService.getDayTimestampFromDate(row.date.substring(0, 10))
        ((row.device_id, (row.capturetime - wholeDay._2) / 120), row)
      }).sortBy(_._1._2)
    logInfo(s"Number of records: ${track2ST.size}")

    //轨迹提纯后，进行乘积量化
    val track2SpaceTime = track2ST
      .groupBy(_._1)
      .mapValues(points => {
        //【2.1】
        val filterPoints = PointService.filterOutliers(points.map(_._2)).sortBy(_.capturetime)
        if (filterPoints.isEmpty) {
          logError("Trajectory filtering error!")
        }
        //利用聚类算法，将该时间片下的轨迹进行聚类
        PointService.pointCluster(filterPoints, 50)
      }).flatMap(r => r._2.map(point => (r._1._1, MysqlTrackService.getDateFromTimeStamp(point._3) ,point._1, point._2)))
    track2SpaceTime.foreach(r => println(s"${r._1},${r._2},${r._3},${r._4}"))

    val encodeTrack = track2SpaceTime.map(row => {
      // 【1.1】创建S2LatLng，经纬度转弧度计算球面坐标
      val latLng = S2LatLng.fromDegrees(row._3, row._4)
      //【1.2】 获取自定义S2时空编码
      val dateTime = MysqlTrackService.getDayTimestampFromDate(row._2,"yyyy-MM-dd HH:mm:ss")._2
      val zoreHourTime = MysqlTrackService.getDayTimestampFromDate(row._2.substring(0, 10))._2
      val spanTime = (dateTime - zoreHourTime)/120
      val myS2Encode = MyS2Encode.fromLatLng(spanTime, latLng).parent(s2_level)
      (row._1, myS2Encode.id())
    })
    encodeTrack.foreach(r => println(s"${r._1},${r._2}"))
  }

}
