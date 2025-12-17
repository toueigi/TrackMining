package com.track.mining.trackmanager.service

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.google.common.geometry.{MyS2Encode, S2CellId, S2LatLng}
import com.track.mining.algorithm.lsh.{VectorLSH, VectorLSHModel}
import com.track.mining.common.Logging
import com.track.mining.trackmanager.bean.Point

import scala.collection.mutable.ArrayBuffer

object TrackLSHService extends Logging {

  /**
    * Query trajectory data based on conditions such as date, device label, distance traveled, and number of movement cells.
    * Quantitatively encode trajectories spatio-temporally using the two parameters timespan and s2_level.
    * @param dateCondition date condition
    * @param filterTag     device label
    * @param moveMinDistanceFilter distance traveled
    * @param distinctS2GridCnt  number of movement s2cells
    * @param timespan
    * @param s2_level
    * @return Array(date,(device_id, Array(track_code)))
    */
  def generateAllTrackVector(dateCondition:String = "", filterTag:String = "", moveMinDistanceFilter:Double
                             , distinctS2GridCnt:Int, timespan:Int, s2_level:Int ): Array[(String, (String, Array[Long]))] = {
    val statTime = System.currentTimeMillis()
    log.info(s"Loading data!!!")
    //【1】Load trajectory data and return：Array[(phonenum, capturetime, lngitude, latitude)]
    val allTargetTrackVecotor = MysqlTrackService
      .dataConvertSpaceTime("", dateCondition,false, filterTag)
      .map(row => {
        ((row.device_id, row.date.substring(0,10)) ,row)
      })
      .groupBy(_._1)
      .toArray
      .map(trackInfo =>  {
        val queryTarget:String =  trackInfo._1._1
        val beginDate:String = trackInfo._1._2
        val code = TrackVector(queryTarget, beginDate, trackInfo._2.map(_._2), timespan, s2_level)
        //【2】calculate the target movement distance
        val moveMaxDistance = PointService.calculateMaxDistance(trackInfo._2.map(_._2).distinct.toArray)
        val distinctS2GridCnt = trackInfo._2.map(_._2).distinct.map(row => {
          //【3】Create S2LatLng, convert latitude and longitude to radians to calculate spherical coordinates
          val latLng = S2LatLng.fromDegrees(row.latitude, row.longitude)
          S2CellId.fromPoint(latLng.toPoint).parent(s2_level)
        }).distinct.size

        ( beginDate, queryTarget, code, moveMaxDistance, distinctS2GridCnt)
      }).filter(r => (r._3 != null && r._3.nonEmpty && r._4 >= moveMinDistanceFilter && r._5 >= distinctS2GridCnt))
        .map(r => (r._1, (r._2, r._3)))
    log.info(s"Completed trajectory loading and implemented trajectory quantization encoding, with a total duration of ${(System.currentTimeMillis() - statTime)/1000} second!!!")
    allTargetTrackVecotor
  }


  /**
    * Perform vector encoding on the trajectory data for the target device corresponding to the specified date.
    * @param queryTarget device_id
    * @param beginDate   query date
    * @param targetTrack
    * @param timespan
    * @param s2_level
    * @return return vector encoding
    */
  def TrackVector(queryTarget:String, beginDate:String, targetTrack: ArrayBuffer[Point], timespan:Int = 1200, s2_level:Int = 16)
  : Array[Long]= {
    val wholeDay = MysqlTrackService.getDayTimestampFromDate(beginDate.substring(0, 10))
    if (targetTrack != null && targetTrack.size > 1) {
      //Perform product quantization on the time-series trajectory data，
      // return：List[((phonenum,timespan),Array((lngitude,latitude), S2CellId))]
      val result = PointService.removeOutlierInTrajectory(targetTrack.toArray)
        .map(row => {
          val wholeDay = MysqlTrackService.getDayTimestampFromDate(row.date.substring(0, 10))
          ((row.device_id, (row.capturetime - wholeDay._2) / timespan), row)
        })
        .sortBy(_._1._2)
        .groupBy(_._1)
        .mapValues(points => {
          val filterPoints = PointService.filterOutliers(points.map(_._2)).sortBy(_.capturetime)
          if (filterPoints.isEmpty) {
            logError("Trajectory filtering error！！！")
          }
          //Using clustering algorithms, group the trajectories within this time slice into clusters.
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
        .map(row => {
          // 【2.1】创建S2LatLng，经纬度转弧度计算球面坐标
          val latLng = S2LatLng.fromDegrees(row.latitude, row.longitude)
          //【2.2】 获取自定义S2时空编码
          val myS2Encode = MyS2Encode.fromLatLng(row.timespan, latLng).parent(s2_level)
          myS2Encode.id()
        })
      result
    }else{
      null
    }
  }


  /**
    * Create test data
    * @return
    */
  def generateTestTrackData(s2_level:Int):Array[(String,Array[Long])] = {
    val tracks = Array(
      ("18923440909","2023-07-04 19:57:07",23.3967826,113.46415649),
      ("18923440909","2023-07-04 19:58:38",24.5967770,113.46419673),
      ("18923440909","2023-07-04 19:58:48",26.896577,113.464189),
      ("18923440909","2023-07-04 19:59:08",23.397729,113.462625),
      ("15676523452","2023-07-04 19:57:07",23.3967826,113.46415649),
      ("15676523452","2023-07-04 19:58:38",23.3967770,113.46419673),
      ("15676523452","2023-07-04 19:58:48",23.396577,113.464189),
      ("15676523452","2023-07-04 19:59:08",23.397729,113.462625),
      ("15676523452","2023-07-04 19:59:45",23.397749,113.462635))

    val beginDate  = "2023-07-04"
    val track2ST = tracks.map(r => {
      val capture_time = MysqlTrackService.dateToTimestamp(r._2)
      new Point(device_id = r._1, latitude = r._3, longitude = r._4, capturetime = capture_time._2, data_type=0 ,date = r._2)
    })
      .map(row => {
        val wholeDay = MysqlTrackService.getDayTimestampFromDate(row.date.substring(0, 10))
        ((row.device_id, (row.capturetime - wholeDay._2) / 120), row)
      })
      .sortBy(_._1._2)
      .groupBy(_._1)
      .mapValues(points => {
        val filterPoints = PointService.filterOutliers(points.map(_._2)).sortBy(_.capturetime)
        //利用聚类算法，将该时间片下的轨迹进行聚类
        PointService.pointCluster(filterPoints, 50)
      })
      .flatMap(r => r._2.map(point => (r._1._1, MysqlTrackService.getDateFromTimeStamp(point._3) ,point._1, point._2)))
      .map(row => {
        // 【1.1】Create S2LatLng, convert latitude and longitude to radians to calculate spherical coordinates
        val latLng = S2LatLng.fromDegrees(row._3, row._4)
        //【1.2】 Obtain custom S2 spatiotemporal encoding
        val dateTime = MysqlTrackService.getDayTimestampFromDate(row._2,"yyyy-MM-dd HH:mm:ss")._2
        val zoreHourTime = MysqlTrackService.getDayTimestampFromDate(row._2.substring(0, 10))._2
        val spanTime = (dateTime - zoreHourTime)/120
        val myS2Encode = MyS2Encode.fromLatLng(spanTime, latLng).parent(s2_level)
        (row._1, myS2Encode.id())
      })
      .groupBy(_._1)
      .map(r => (r._1,r._2.map(_._2).toArray)).toArray
    track2ST
  }

  /**
    * Detect the group move pattern .
    * @param LSHRS     to persist the result
    * @param startDate query start date
    * @param endDate   query end date
    * @param filterTag device filter
    * @param timespan  timespan
    * @param computeBruteForce if compute the cost time of BruteForce
    */
  def groupCommMovementAnalyse(LSHRS: ArrayBuffer[(String, String, String, Int)], startDate:String, endDate:String, filterTag:String
                               , moveMinDistanceFilter:Double = 2000.0, distinctGeoCnt:Int = 5
                               , timespan:Int = 120, s2_level:Int = 16, computeBruteForce:Int = 0)
  :( ArrayBuffer[(String, String, String, Double)], ArrayBuffer[(String, Long, Long)] ) ={
    //Array[(String, (String, Array[Long]))]
    val jaccardRs = new ArrayBuffer[(String, String, String, Double)]()
    val costTimes = new ArrayBuffer[(String, Long, Long)]()
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    // Generate date sequence (including start and end dates)
    val dateSeq = Iterator
      .iterate(LocalDate.parse(startDate, formatter))(_.plusDays(1))
      .takeWhile(!_.isAfter(LocalDate.parse(endDate, formatter)))
      .map(_.format(formatter))
      .toSeq
    val allDate = dateSeq.sliding(2).map(_.mkString(",")).toArray.sorted
    allDate.foreach(day => {
      //return : Array(filterDate,(deviceid, Array(track_code)))
      val allTargetTrackVecotorRdd = generateAllTrackVector(day, filterTag, moveMinDistanceFilter, distinctGeoCnt, timespan, s2_level)
      log.info(s"DATE:${day}, Need to compute similarity devices count :${allTargetTrackVecotorRdd.size}")

      val LSHStartTime = System.currentTimeMillis()
      analyseLSH(allTargetTrackVecotorRdd, LSHRS)
      val lshCostTime = (System.currentTimeMillis() - LSHStartTime)
      log.info(s"DATE:${day},The similarity of all trajectories was calculated using LSH, with a total processing time of ${lshCostTime} millisecond！！！")

      val jaccardStartTime = System.currentTimeMillis()
      if(computeBruteForce > 0 ){
        analyseJaccard(allTargetTrackVecotorRdd).map(r => {
          jaccardRs.append((r._1, r._2, r._3, r._4))
        })
      }
      val jaccardCostTime = (System.currentTimeMillis() - jaccardStartTime)
      log.info(s"Based on Jaccard similarity to complete all trajectory comparisons, the total time taken was ${jaccardCostTime} millisecond")
      costTimes.append( (day.split(",")(0), lshCostTime, jaccardCostTime) )
    })

    (jaccardRs, costTimes)
  }

  /**
    *  The entrance method of LSH
    * 【1】Collect all users' vectorized trajectories with parameters of timespan intervals and S2-level grid resolution;
    * 【2】Obtain all vectorized codes and identify the largest integer prime number
    * 【3】Apply hash encoding to the vectorized codes
    * 【4】Construct clusters using minhash
    * 【5】Identify potential companion numbers based on clustering results
    * @param allTargetTrackVecotorRdd: Array(filterDate,(deviceid, Array(track_code)))
    * @return
    */
  def analyseLSH(allTargetTrackVecotorRdd: Array[(String, (String, Array[Long]))]
                 , resultArray:ArrayBuffer[(String, String, String, Int)] ):Unit= {

    //List of results stored for LSH: ArrayBuffer(date, lshRs, Array(vectorId, deviceId))
    val lshList = new ArrayBuffer[( String, VectorLSHModel, Array[(Int,String)] )]
    //【1】Iterate through daily data to construct the data format required for Minhash.
    allTargetTrackVecotorRdd
      .groupBy(_._1)
      .mapValues(row => row.map(r => r._2))
      .toArray
      .foreach(row => {
        //return :（lshRs, Array(vectorIdx, deviceid)）
        val lsh = LSHCore(row._2)
        //return :(date, lshRs, Array(vectorIdx, deviceid))
        lshList.append((row._1, lsh._1 , lsh._2))
      })
    //【2】Identify device clusters exhibiting similar daily patterns
    lshList.sortBy(_._1).foreach(row =>{
      if(row._2.cluster_vector.size > 0){
        val id2Index = row._3.toMap //Correspondence between device index code and their original device_no (mobile phone numbers)
        row._2.cluster_vector.groupBy(_._1).map { case (k, v) => {
          v.map(_._2).combinations(2).foreach(elem => {
            print("")
            val dA = id2Index.get(elem(0)).get
            val dB = id2Index.get(elem(1)).get
            if (dA <= dB) {
              resultArray.append((dA, dB, row._1, k))
            } else {
              resultArray.append((dB, dA, row._1, k))
            }
          })
        }}
      }
    })
  }

  /**
    * The core method of LSH
    * @param targetsTrack  Array(deviceid, Array(track_code))
    * @return （lshRs, Array(vectorIdx, deviceid)）
    */
  def LSHCore(targetsTrack:Array[(String,Array[Long])], numRows:Int = 27 * 4, numBands:Int = 27 ):(VectorLSHModel,Array[(Int,String)]) = {
    //Obtain the deduplicated set of codes
    val vector2Id = targetsTrack.flatMap(r => r._2).distinct.zipWithIndex.toMap
    //Encode trajectory data for each target to facilitate subsequent traceability.
    val targetVectorWithId = targetsTrack.map(r => {
      val vector2Ids =  r._2.map(vector => {
        vector2Id.get(vector).get
      })
      (r._1, vector2Ids)
    }).zipWithIndex
    val primeN = TrackLSHService.primeNum(vector2Id.size)
    //Constructing LSH-like objects
    val vectorLSH = new VectorLSH(targetVectorWithId.map(r => (r._1._2, r._2)), p = primeN, m = 1, numRows, numBands, minClusterSize = 2)
    //println(s"相似度为${vectorLSH.jaccard(targetVector.map(r => (r._1._2)))}")
    //println(vectorLSH.getB(4, 0.4, 0.8))
    (vectorLSH.run(), targetVectorWithId.map(row => (row._2, row._1._1)))
  }

  /**
    * The entrance of universal brute-force comparison algorithm,
    * calculate the jaccard similarity between each pair.
    *
    * @param allTargetTrackVecotorRdd
    * @return similar device pairs :Array[(device_1,divice_2,date,similarity)]
    */
  def analyseJaccard(allTargetTrackVecotorRdd: Array[(String, (String, Array[Long]))] )
  :Array[(String, String, String, Double)] = {
    val jaccardSimilar = new ArrayBuffer[( String, String, String, Double)]
    allTargetTrackVecotorRdd
      .groupBy(_._1)
      .mapValues(row => row.map(r => r._2))
      .toArray
      .foreach(row => {
        val date = row._1
        for(i <- row._2.indices){
          for(j <- (i+1 until row._2.size)){
            val ret  = TrackLSHService.jaccard(row._2(i), row._2(j))
            if(ret._3 >= 0.4)
              jaccardSimilar.append( (ret._1, ret._2, date, ret._3 ))
          }
        }
      })
    jaccardSimilar.toArray
  }



  /**
    * Find the smallest prime number greater than a given number
    * @param maxIndex Specified number of seeds
    * @return The largest prime number greater than the given seed number
    */
  def primeNum(maxIndex: Int): Int = {
    def isPrime(num: Int) = {
      var flag = true
      for (i <- (2 to Math.floor(Math.sqrt(num)).toInt) if flag) {
        if (num % i == 0) flag = false
      }
      flag
    }
    var minPrimeNum = maxIndex + 1
    while (!isPrime(minPrimeNum)) {
      minPrimeNum += 1
    }
    minPrimeNum
  }

  /**
    * Calculate the similarity between the trajectory codes of two devices
    * @param a
    * @param b
    * @return
    */
  def jaccard(a : (String,Array[Long]), b : (String,Array[Long]))  = {
    if(a._1 <= b._1)
      (a._1, b._1, a._2.intersect(b._2).distinct.size / a._2.union(b._2).distinct.size.doubleValue)
    else
      (b._1, a._1, a._2.intersect(b._2).distinct.size / a._2.union(b._2).distinct.size.doubleValue)
  }

}
