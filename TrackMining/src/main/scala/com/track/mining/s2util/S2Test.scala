package com.track.mining.s2util

import com.google.common.geometry.{MyS2Encode, S2CellId, S2LatLng, S2Point}
import org.roaringbitmap.RoaringBitmap

object S2Test {

  val pciBit = 14
  val ssbIdxBit = 7
  val rsrqLevelBit = 3
  val userCntBit = 8
  /**
    * 注意：以下方式为简单的测试方法，具体每个特征的占位长度需要根据统计的结果而定，同时，针对部分超出长度的特征需要进行try catch 操作。
    * @param array :Array[(nearPCI:Int,ssbIdx:int,rsrqLevel:String,userCnt:Int)])
    * @return
    */
  def featureToVector(array: Array[(Int,Int,String,Int)]):RoaringBitmap = {
    val bitMap = new RoaringBitmap()
    for(featureInfo <- array){
      val rsrqVal = featureInfo._3.toUpperCase match {
        case "A" => 0
        case "B" => 1
        case "C" => 2
        case "D" => 3
        case "E" => 4
        case _   => 5  // 默认情况
      }
      val nearPCI    = featureInfo._1 << (ssbIdxBit + rsrqLevelBit + userCntBit)//往高位移动
      val ssbIdx     = featureInfo._2 << (rsrqLevelBit + userCntBit)
      val rsrqLevel = rsrqVal << rsrqLevelBit
      val userCnt    = log3(featureInfo._4).toInt
      val vector  =  nearPCI | ssbIdx | rsrqLevel | userCnt
      bitMap.add(vector)
    }
    bitMap.runOptimize()
    bitMap
  }

  // 计算 log₃(x)
  def log3(x: Double): Double = {
    Math.log(x) / Math.log(3)
  }

  /**
    * 该函数用于获取某个INT数的高3位和最大位数级别。其中最大位数级别包括（0，1，2，3）四种类型
    * 0代表：该数属于百位数以内；1：代表为千位数；2：代表万位数；3：代表十万位数。
    * 比如：987返回（987，0）；9876返回（987，1）；897012返回（897，3）；9872908返回（999，3）
    * 最终，可利用该函数返回的结果存储在一个10+2的bit位里面。10位存储前面的三位数，2位存储该数的级别。
    * @param num
    * @return
    */
  def getHighestDigitInfo(num: Int): (Int, Int) = {
    if (num == 0) {
      return (1, 0) // 0 的情况特殊处理
    }
    val numStr = num.toString
    val highestDigitPlace = numStr.length // 最高位数级别
    val (topThreeDigits, highPoslevel) = {
      if (highestDigitPlace >= 7){
        (999, 3)
      }else if (highestDigitPlace >= 3) {
        (numStr.substring(0, 3).toInt, highestDigitPlace - 3)
      }else {
        (num, 0) // 如果数字不足 3 位，直接返回整个数字
      }
    }
    (topThreeDigits, highPoslevel)
  }

  def main(args: Array[String]): Unit = { // 示例经纬度
    val latitude = 37.7749
    val longitude = 122.4194
    // 选择一个cell级别，级别越高，cell越小
    val level = 20
    //【1】地球经纬度转化为球面坐标的代码步骤，实现(lat,lng) --> f(x,y,z)
    // 【1.1】创建S2LatLng，经纬度转弧度计算球面坐标
    val latLng = S2LatLng.fromDegrees(latitude, longitude)
    // 【1.2】转换为S2Point，球面的点转化为球面坐标
    val point = latLng.toPoint
    //【2】球面坐标转化为平面坐标，实现f(x,y,z) --> g(face,u,v)，
    // face是正方形的六个面，u，v对应的是六个面中的一个面上的x，y坐标。
    val cellId = S2CellId.fromPoint(point).parent(level)
    //以上两行代码等价于以下一行代码
    //S2CellId cellId = S2CellId.fromLatLng(latLng).parent(level);
    // 生成S2CellId
    //S2CellId cellId = S2CellId.fromLatLng(latLng);
    // 输出Cell ID
    val cellId2 = cellId.parent(20)
    System.out.println("S2 Cell ID: " + cellId2.id)

    val cellId3 = cellId.parent(19)
    System.out.println("S2 Cell ID: " + cellId3.id)
    val myS2Encode = MyS2Encode.fromS2CellId(2880L, cellId2)
    System.out.println("My EncodeS2 ID: " + myS2Encode.id)
    System.out.println("My EncodeS2 ID: " + myS2Encode.parent(19).id)
    val myS2Encode3 = MyS2Encode.fromS2CellId(2880L, cellId3)
    System.out.println("My EncodeS2 ID: " + myS2Encode3.id)
  }
}
