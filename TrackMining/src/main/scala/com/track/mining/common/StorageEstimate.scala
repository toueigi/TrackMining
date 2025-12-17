
import com.track.mining.HphmCompress
import org.apache.spark.util.SizeEstimator

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.runtime.ScalaRunTime._
import scala.reflect.runtime.universe._

object StorageEstimate {
  def main(args: Array[String]): Unit = {
    // Define the array
    val arrayString: ArrayBuffer[String] = new ArrayBuffer[String] // Example array of 1000 integers
    val arrayLong: ArrayBuffer[Long] = new ArrayBuffer[Long] // Example array of 1000 integers
    (0 to 20000000).foreach( r => {
      arrayString.append(HphmCompress.generateRandomPlate)
    })

    /*(0 to 10000000).foreach( r => {
      arrayLong.append(HphmCompress.generateRandomPhoneNumber.toLong)
    })*/
    // Output the result
    println(s"Storage size of String Array : ${SizeEstimator.estimate(arrayString.toArray)} bytes")
    println(s"Storage size of Long   Array : ${SizeEstimator.estimate(arrayLong.toArray)} bytes")
  }
}
