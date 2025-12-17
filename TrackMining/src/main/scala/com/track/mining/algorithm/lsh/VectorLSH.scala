package com.track.mining.algorithm.lsh


/**
  * Construct a local Minhash class
  * @param data Raw data (Array[trajectory_code, the encoded trajectory of this device])
  * @param p The smallest prime number greater than the total number of unique trajectory codes
  * @param m This parameter defaults to 1 and is not required
  * @param numRows  Total number of HASHes to generate. Higher values increase hit probability but also performance overhead
  * @param numBands Number of bandwidths used in LSH. Higher values make hits harder to achieve, or put differently, higher data dispersion yields more reliable hits
  * @param minClusterSize Minimum number of clusters for filtering
  */
class VectorLSH(data : Array[(Array[Int], Int)],
          p : Int, m : Int, numRows : Int, numBands : Int,
          minClusterSize : Int) extends Serializable {

  /** run LSH using the constructor parameters */
  def run() : VectorLSHModel = {

    //create a new model object
    val model = new VectorLSHModel(p, m, numRows)

    //preserve vector index
    val zdata = data
    //      .zipWithIndex().cache()

    // compute signatures from matrix
    // - hash each vector <numRows> times
    // - position hashes into bands. we'll later group these signature bins and has them as well
    //this gives us ((vector idx, band#), minhash)
    val signatures = zdata.flatMap(v => model.hashFunctions.flatMap(h => List(((v._2, h._2 % numBands),h._1.minhash(v._1)))))

    //reorganize data for shuffle
    //this gives us ((band#, hash of minhash list), vector id)
    //groupByKey gives us items that hash together in the same band
    model.bands = signatures.groupBy(_._1).map(x => {
      val bandId   = x._1._2
      val hashList = x._2.map(_._2).mkString("-")
      val vectorId = x._1._1
      (bandId, hashList, vectorId)
    }).map(row => {
      ((row._1,row._2),row._3)
    }).groupBy(_._1).mapValues(r => r.map(_._2))

    //we only want groups of size >= <minClusterSize>
    //(vector id, cluster id)
    model.vector_cluster = model.bands.filter(x => x._2.size >= minClusterSize)
      .map(x => x._2.toList.sorted).toArray
      .distinct
      .zipWithIndex
      .map(x => x._1.map(y => (y.asInstanceOf[Int], x._2)))
      .flatMap(x => x.grouped(1)).map(x => x(0))

    //(cluster id, vector id)
    model.cluster_vector = model.vector_cluster.map(x => x.swap)

    //(cluster id, List(vector))
    //model.clusters = zdata.map(x => x.swap).join(model.vector_cluster).map(x => (x._2._2, x._2._1)).groupByKey().cache()
    val keyVal = zdata.map(x => x.swap).toMap
    model.clusters   = model.vector_cluster.map {
      case (key, value) =>
        // 如果map1中存在对应的键，则将两个数组的值组合在一起
        keyVal.get(key) match {
          case Some(existingValue) => (value, existingValue)
          case None => (value,null) // 如果没有找到对应的键，可以返回null或者其它默认值
        }
    }.groupBy(_._1).map(r=> (r._1, r._2.map(_._2))).toArray

    //compute the jaccard similarity of each cluster
    //model.scores = model.clusters.map(row => (row._1, jaccard(row._2)))

    model
  }

//  /** compute a single vector against an existing model */
//  def compute(data : Array[Int], model : VectorLSHModel, minScore : Double) : Array[(Int, Array[Array[Int]])] = {
//    model.clusters.map(x => (x._1, x._2 ++ Array(data))).filter(x => jaccard(x._2) >= minScore)
//  }

  /** compute jaccard between two vectors */
  def jaccard(a : (String,Array[Long]), b : (String,Array[Long])) : Double = {
    a._2.intersect(b._2).distinct.size / a._2.union(b._2).distinct.size.doubleValue
  }

//  /** compute jaccard similarity over a list of vectors */
//  def jaccard(l : Array[Array[Int]]) : Double = {
//    l.foldLeft(l(0).indices.toList)((a1, b1) => a1.intersect(b1.indices.toList.asInstanceOf[List[Nothing]])).size /
//      l.foldLeft(List())((a1, b1) => a1.union(b1.indices.toList.asInstanceOf[List[Nothing]])).distinct.size.doubleValue
//  }


  /**
    * 计算 b 个桶, 每个桶 r 个hash 函数时, 相似度为 s 的两个集合被分到一个桶内且这个桶r个hash值都相等的概率
    * @return  probability
    */
  def getS(s: Double, r: Int, b: Int): Double = {
    1 - Math.pow(1 - Math.pow(s, r), b)
  }

  /**
    * The similarity threshold at which the probability undergoes a drastic change
    * when calculating b buckets with r hash functions per bucket.
    * @return  similarity
    */
  def getT(r: Int, b: Int): Double = {
    Math.pow(1.0 / b, 1.0 / r)
  }

  /**
    * Calculate the number of buckets b required such that, for each bucket containing r hash functions
    * , the probability that two sets satisfying similarity s
    * are assigned to the same bucket and all r hash values in that bucket are equal is p.
    * @return  bucket
    */
  def getB(r: Int, s: Double, p: Double): Long = {
    Math.round(Math.log(1 - p) / Math.log(1 - Math.pow(s, r)))
  }

  def getL(r: Int, s1: Double, s2: Double, p1: Double, p2: Double): Unit = {
    println("s1 " + getB(r, s1, p1))
    println("s2 " + getB(r, s2, p2))
  }
}