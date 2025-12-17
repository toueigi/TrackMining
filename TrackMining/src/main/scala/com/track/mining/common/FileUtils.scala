package com.track.mining.common

import java.io.{BufferedReader, File, FileInputStream, InputStream, InputStreamReader, PrintWriter}
import java.nio.charset.StandardCharsets

import scala.io.{BufferedSource, Source}

object FileUtils {

  /**
    * Merge all files in the specified path into a single designated file.
    * @param sourceFilePath
    * @param destFile
    */
  def mergeFile(sourceFilePath:File , destFile:File): Unit ={
    val write = new PrintWriter(destFile)
    writeFile(sourceFilePath, write)
    write.close()
  }

  def writeFile(sourceFilePath:File , write:PrintWriter):Unit = {
    for(sourceFile <- sourceFilePath.listFiles()){
      if(sourceFile.isFile){
        val source: BufferedSource = Source.fromFile(sourceFile)
        for (line <- source.getLines()){
          write.println(line)
        }
        source.close()
      }else{
        writeFile(sourceFile, write)
      }
    }
  }
  def main(args: Array[String]): Unit = {
    val sourceFilePath = new File("D:\\zhangws\\coding\\OfflineMinder\\workspace\\spark-2.1.0\\data\\recruit\\source")
    val destFile:File = new File("D:\\zhangws\\coding\\OfflineMinder\\workspace\\spark-2.1.0\\data\\recruit\\dest\\recruit_train.txt")
    if(destFile.exists()){
      destFile.delete()
      destFile.createNewFile()
    }
    mergeFile(sourceFilePath , destFile)
  }
}

