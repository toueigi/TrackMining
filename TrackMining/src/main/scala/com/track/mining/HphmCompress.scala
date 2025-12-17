package com.track.mining

import scala.util.Random

object HphmCompress {
    private val BASE = 70 // 假设车牌只包含大写字母和数字，总共36个字符

    private val CHARS = "#ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789京沪津渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼台*"

    // 省份简称列表（示例，未包含全部省份）
    val provinces = List("京", "沪", "津", "渝", "冀", "豫", "云", "辽", "黑", "湘", "皖", "鲁", "新", "苏", "浙", "赣", "鄂", "桂", "甘", "晋", "蒙", "陕", "吉", "闽", "贵", "粤", "青", "藏", "川", "宁", "琼")

    // 生成随机字母（A-Z）
    def randomLetter: Char = ('A' to 'Z').apply(Random.nextInt(26))
    // 生成随机数字（0-9）
    def randomDigit: Char = ('0' to '9').apply(Random.nextInt(10))

    // 生成随机车牌序列号（5位，可以是字母和数字的组合）
    def randomPlateSerial: String = (1 to 7).map(r => if (Random.nextBoolean()) randomLetter else randomDigit).mkString

    // 生成随机车牌号码
    def generateRandomPlate: String = {
      val province = provinces(Random.nextInt(provinces.size))
      val serial = randomPlateSerial
      s"$province$serial"
    }

    // 生成随机手机号码第二位数字（3-9）
    def randomSecondDigit: Char = ('3' to '9').apply(Random.nextInt(7))

    // 生成随机手机号码
    def generateRandomPhoneNumber: String = {
      val firstDigit = '1' // 手机号码第一位固定为1
      val secondDigit = randomSecondDigit // 随机生成手机号码第二位数字
      val remainingDigits = (1 to 9).map(_ => randomDigit).mkString // 随机生成后面9位数字
      s"$firstDigit$secondDigit$remainingDigits"
    }

    // 将车牌号码转化为整数编码
    def encode(licensePlate: String): Long = {
      var result = 0L
      var factor = 1L
      var i = licensePlate.length - 1
      while ( i >= 0) {
        val c = licensePlate.charAt(i)
        var index = getCharIndex(c)
        if (index == -1) throw new IllegalArgumentException("Invalid character in license plate: " + c)
        result += index * factor
        println(s"第${i}位的索引为：${index},编码为：${index * factor}")
        factor *= BASE
        i = i - 1
      }
      result
    }

    // 从整数编码恢复车牌号码
    def decode(encodedValue: Long): String = {
      val sb = new StringBuilder
      var decodeVal = encodedValue
      var i = 0
      while (decodeVal > 0) {
        val index = decodeVal % BASE
        println(s"第${i}位的索引为：${index}")
        val c = CHARS.charAt(index.toInt)
        sb.insert(0, c)
        decodeVal = decodeVal / BASE
        i = i+1
      }
      sb.toString
    }

    // 获取字符在预定义字符串中的索引
    private def getCharIndex(c: Char) = CHARS.indexOf(c)

    def main(args: Array[String]): Unit = {
      println(provinces.mkString(""))
      val licensePlate = "WJ粤AA094"
      val encodedValue = encode(licensePlate)
      System.out.println("Encoded value: " + encodedValue)
      val decodedLicensePlate = decode(encodedValue)
      System.out.println("Decoded license plate: " + decodedLicensePlate)
    }
}
