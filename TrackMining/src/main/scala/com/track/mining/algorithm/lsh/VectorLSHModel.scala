package com.track.mining.algorithm.lsh

import scala.collection.mutable.ListBuffer


class VectorLSHModel(p : Int, m : Int, numRows : Int) extends Serializable {
  
  /** generate rows hash functions */
  private val _hashFunctions = ListBuffer[VectorHasher]()
  for (i <- 0 until numRows)
    _hashFunctions += VectorHasher.create(p, m)
  final val hashFunctions : List[(VectorHasher, Int)] = _hashFunctions.toList.zipWithIndex

  /** the "bands" ((hash of List, band#), row#) */
  var bands : Map[(Int, String), Iterable[Int]] = _
  
  /** (vector id, cluster id) */
  var vector_cluster : Array[(Int, Int)] = _
  
  /** (cluster id, vector id) */
  var cluster_vector : Array[(Int, Int)] = _
  
  /** (cluster id, List(Vector) */
  var clusters : Array[(Int, Array[Array[Int]])] = _
 
  /** jaccard cluster scores */
  var scores : Array[(Int, Double)] = _

  
}