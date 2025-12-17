package com.track.mining.algorithm.lsh

import scala.util.Random

/**
 * simple hashing function. defined by ints a, b, p, m 
 * where a and b are seeds with a > 0.
 * p is a prime number, >= u (largest item in the universe)
 * m is the number of hash bins
 */
class VectorHasher(a : Int, b : Int, p : Int, m : Int) extends Serializable {

  override def toString(): String = "(" + a + ", " + b + ")"

  def hash(x : Int) : Int = {
    (((a.longValue() * x) + b) % p ).intValue
  }

  def minhash(v : Array[Int]) : Int = {
    val minhash = v.map(i => hash(i)).min
    //println(s"#####${minhash}")
    minhash
  }
  
}

object VectorHasher {
  /** create a new instance providing p and m. a and b random numbers mod p */
  def create(p : Int, m : Int) = {
    val val_a = a(p)
    val val_b = b(p)
    val hasher = new VectorHasher(val_a, val_b, p, m)
    //println(hasher.toString())
    hasher
  }



  /** create a seed "a" */
  def a(p : Int) : Int = {
    val r = new Random().nextInt(p)
    if(r == 0)
      a(p)
    else
      r
  }

  /** create a seed "b" */
  def b(p : Int) : Int = {
    new Random().nextInt(p)
  }

}


