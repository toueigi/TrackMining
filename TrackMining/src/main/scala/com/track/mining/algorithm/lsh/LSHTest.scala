package com.track.mining.algorithm.lsh

import java.io.File

import com.track.mining.trackmanager.DataWarehouse

import scala.collection.mutable.ArrayBuffer

object LSHTest {
  val outFile: File = new File("data\\output\\chapter5.3\\figures")

  def main(args: Array[String]): Unit = {
    println(getS(0.4, 4, 65 * 4))
//    val result = new ArrayBuffer[(Float, Double)]
//    for(value <- (1 to 100)){
//      val tmp = value/100.0
//      result.append( (value, getS(tmp, 4, 27)) )
//    }
//
//    DataWarehouse.saveAsTxtFile(outFile + File.separator + s"Figure6_Probability_Curve_of_Locality-Sensitive_Hashing.txt", result.sortBy(_._1).map(r => s"${r._1}, ${r._2}").toArray)
  }

  /**
    *
    * @param s  相似度
    * @param r  band with
    * @param b  桶数
    * @return   命中概率
    */
  def getS(s: Double, r: Int, b: Int): Double = {
    1 - Math.pow(1 - Math.pow(s, r), b)
  }

}
