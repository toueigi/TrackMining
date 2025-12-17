package com.track.mining.algorithm.cluster

import java.text.DecimalFormat

import org.roaringbitmap.RoaringBitmap

import scala.collection.mutable.ArrayBuffer

object Evaluation {
  /**
    * 计算集合中每个样本到其他样本的相似度
    * @param samples
    */
  def getEps(samples: List[(String, RoaringBitmap, String)]
             ) = {
    val result = new ArrayBuffer[(Int,Double)]
    //val result = new ArrayBuffer[(Int,Double)]
    val ft = new DecimalFormat("#.#####")
    var id = 0
    for(text1 <- samples){
      val totalScore = new ArrayBuffer[Double]()
      for(text2 <-samples){
        val distance = 1.0D - RoaringBitmap.and(text1._2,text2._2).toArray.size.toDouble/RoaringBitmap.or(text1._2,text2._2).toArray.size
        totalScore.append(ft.format(distance).toDouble)
      }
      if(totalScore.filter(r => r > 0D && r < 1D).sorted.toArray.take(10).nonEmpty)
        result.append((id, totalScore.filter(r => r > 0D && r < 1D).sortBy(line => line).toArray.take(5).last))
      id =id + 1
    }
    result
  }

  def getSCoef(result: Seq[GDBSCAN.Cluster], minPts:Int) = {
    val coefArray = new ArrayBuffer[Double]()
    for( center <- result){
      this.silhouetteCoefficient(result, center, coefArray)
    }
    println(s"样本量为${coefArray.size},聚类核心参数MinPts为${minPts},其对应的SCoef指数为${coefArray.sum.toDouble/coefArray.size}")
  }

  /**
    *
    * @param result  聚类后的结果
    * @param center  指定测试的类
    * @return
    */
  def silhouetteCoefficient(result: Seq[GDBSCAN.Cluster], center: GDBSCAN.Cluster, coefArray:ArrayBuffer[Double]) = {
    var sameClusterSum: Double = 0.0
    var otherClusterSum: Double = 0.0
    var min: Double = Double.MaxValue
    //非同一个聚类的比较
    for(point <- center.points){
      var j: Int = 0
      for(cluster <- result; if(cluster.id != center.id)){
        j = j+1
        //计算该文本与其他每个类中样本的平均距离
        otherClusterSum += euclideanDistance(cluster, point)
      }
      //平均距离取最小值
      min = if(min < otherClusterSum.toDouble/result.size) min else otherClusterSum.toDouble/result.size //非同一cluster里面的最短distance
      sameClusterSum = euclideanDistance(center, point)
      val coef  = (min - sameClusterSum)/Math.max(sameClusterSum , min)
      coefArray.append(coef)

    }
  }

  def euclideanDistance(cluster: GDBSCAN.Cluster, point:GDBSCAN.Text):Double = {
    var i = 0
    var distance = 0.0
    for(otherClusterPoint <- cluster.points if !otherClusterPoint.wholeWord.equals(point.wholeWord)){
      i = i+1
      distance = distance + (1 - RoaringBitmap.and(point.vector,otherClusterPoint.vector).toArray.size.toDouble/RoaringBitmap.or(point.vector,otherClusterPoint.vector).toArray.size)
    }
    if(i==0){
      distance
    }else{
      distance/i
    }
  }

  /*{
    RoaringBitmap.and(a,b).toArray.size.toDouble/RoaringBitmap.or(a,b).toArray.size
  }*/

}
