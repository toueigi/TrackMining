package com.track.mining.database

import java.util.Properties
import java.io.{FileInputStream, InputStreamReader}
import scala.util.{Try, Using}

object DatabaseConfig {
  private val properties = new Properties()
  private val configFileName = "database.properties"

  init()

  private def init(): Unit = {
    Try {
      // 从类路径加载（推荐方式）
      val inputStream = getClass.getClassLoader.getResourceAsStream(configFileName)
      if (inputStream != null) {
        Using(inputStream) { is =>
          properties.load(new InputStreamReader(is, "UTF-8"))
        }
        println(s"JDBC Configuration file loaded successfully.: $configFileName")
      } else {
        // 尝试从文件系统加载
        val file = new java.io.File(configFileName)
        if (file.exists()) {
          Using(new FileInputStream(file)) { fis =>
            properties.load(new InputStreamReader(fis, "UTF-8"))
          }
          println(s"Load JDBC configuration files from the file system: $configFileName")
        } else {
          println(s"Warning: JDBC configuration file not found $configFileName，Use default configuration")
          setDefaultProperties()
        }
      }
    }.recover {
      case ex: Exception =>
        println(s"Error occurred while loading the JDBC configuration file.: ${ex.getMessage}")
        setDefaultProperties()
    }
  }

  private def setDefaultProperties(): Unit = {
    properties.setProperty("db.driver", "com.mysql.jdbc.Driver")
    properties.setProperty("db.url", "jdbc:mysql://192.168.3.15:3306/TrackMananger")
    properties.setProperty("db.username", "woolsam")
    properties.setProperty("db.password", "thinker@123")
    properties.setProperty("db.pool.initialSize", "5")
    properties.setProperty("db.pool.maxTotal", "20")
  }

  def getDriver: String = properties.getProperty("db.driver")
  def getUrl: String = properties.getProperty("db.url")
  def getUsername: String = properties.getProperty("db.username")
  def getPassword: String = properties.getProperty("db.password")
  def getPoolInitialSize: Int = properties.getProperty("db.pool.initialSize", "5").toInt
  def getPoolMaxTotal: Int = properties.getProperty("db.pool.maxTotal", "20").toInt


  def getProperty(key: String): Option[String] = Option(properties.getProperty(key))
  def getProperty(key: String, defaultValue: String): String =
    properties.getProperty(key, defaultValue)


  def printAllProperties(): Unit = {
    println("=== Database Configuration  ===")
    properties
  }
}