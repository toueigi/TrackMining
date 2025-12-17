package com.track.mining.trackmanager

import java.io.{File, PrintWriter}

import com.track.mining.trackmanager.TrackDataCompress.outStatFile
import com.track.mining.trackmanager.bean.Point

import scala.collection.mutable.ArrayBuffer

object DataWarehouse {

  /**
    * Save the original trajectory, product-quantized trajectory, and coded trajectory separately as .txt files.
    * @param targetTrack     original trajectory
    * @param track2SpaceTime product-quantized
    * @param timespaceEncode coded trajectory
    * @param txtFileName file name
    */
  def saveTrackAsTxt(targetTrack:ArrayBuffer[Point], track2SpaceTime:Array[Point]
                     , timespaceEncode:Array[(String,Long)], txtFileName:String):
  Unit = {

    val origTrack = targetTrack.map(row => {
      s"${row.device_id},${row.capturetime},${row.longitude},${row.latitude}"
    })
    this.saveAsTxtFile(outStatFile + File.separator + "origTrack" + File.separator + txtFileName, origTrack.toArray)

    val spaceTimeTrack = track2SpaceTime.map(row => {
      s"${row.device_id},${row.timespan},${row.s2cell}"
    })
    this.saveAsTxtFile(outStatFile + File.separator + "spaceTimeTrack" + File.separator + txtFileName, spaceTimeTrack.toArray)

    val vectorTrack = timespaceEncode.map(row => {
      s"${row._1},${row._2}"
    })
    this.saveAsTxtFile(outStatFile + File.separator + "vectorTrack" + File.separator + txtFileName, vectorTrack.toArray)

  }


  def saveAsTxtFile(filePath:String, data:Array[String]):Unit = {
    //Use PrintWriter to write to files securely
    var pw:PrintWriter = null
    try {
      val file = new File(filePath)
      //If the file path does not exist, create the path.
      if(!file.getParentFile.exists()){
        file.getParentFile.mkdirs()
      }
      //If the file already exists, recreate it.
      if(file.exists()){
        file.delete()
      }
      file.createNewFile()
      pw = new PrintWriter(file)
      data.foreach(pw.println)
    } finally {
      if(pw != null) pw.close()
    }
  }
}
