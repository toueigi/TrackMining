package com.track.mining.algorithm.cluster

import scala.collection.mutable.{ListBuffer, Set => MutableSet}
import GDBSCAN._
import org.roaringbitmap.RoaringBitmap

/**
 * A clustering algorithm for density based clustering.
 *
 * @author Nepomuk Seiler
 * @param getNeighbours - determine the neighbours at the given point
 * @param isCorePoint - determine if this point is a core point based on its neighbourhood
 * @see http://en.wikipedia.org/wiki/DBSCAN
 * @see http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.71.1980
 */
class GDBSCAN(
  getNeighbours: (Text, Seq[Text]) => Seq[Text],
  isCorePoint: (Text, Seq[Text]) => Boolean) {

  /**
   * Start clustering
   *
   * @param data - each row is treated as a feature vector
   * @return clusters - a list of clusters with
   */
  def cluster(data: List[(String, RoaringBitmap, String)]): Seq[Cluster] = {
    // Visited - using row indices
    val visited = MutableSet[Text]()
    val clustered = MutableSet[Text]()

    // Init points
    //val points = for (row <- 0 until data.size)
    //  yield Point(row)(data(row, ::).inner)

    val points = data.zipWithIndex.map(r => Text(r._1._1,r._1._2,r._1._3,r._2))

    //points.map(row => println(row._1.toString()))
    // Start clustering
    points.collect {
      case point @ (Text(wholeWord, vector, anno, row)) if !(visited contains point) =>
        val neighbours = getNeighbours(point, points.filterNot(_.index == point.index))
        if (isCorePoint(point, neighbours)) {
          visited add point
          val cluster = Cluster(point.index)
          expand(point, neighbours, cluster)(points, visited, clustered)
          Some(cluster)
        } else {
          None // noise
        }
    }.flatten // remove noise
  }

  private def expand(point: Text, neighbours: Seq[Text], cluster: Cluster)(implicit points: Seq[Text], visited: MutableSet[Text], clustered: MutableSet[Text]) {
    cluster add point
    clustered add point
    neighbours.foldLeft(neighbours) {
      case (neighbourhood, neighbour @ Text(wholeWord, vector, anno, row)) =>
        // if not visited yet, create a new neighbourhood
        val newNeighbours = if (!(visited contains neighbour)) {
          visited add neighbour
          getNeighbours(neighbour, points.filterNot(_.index == neighbour.index))
        } else {
          Seq.empty
        }
        // Add to cluster if neighbour point isn't assigned to a  cluster yet
        if (!(clustered contains neighbour)) {
          cluster add neighbour
          clustered add neighbour
        }
        // if the neighbour point is a cluster, join the neighbourhood
        if (isCorePoint(neighbour, neighbourhood)) neighbourhood ++ newNeighbours else neighbourhood
    }

  }
}

object GDBSCAN {

  case class Text(wholeWord:String, vector:RoaringBitmap, anno:String, index:Long){
    override def toString() = s"[$anno]: $wholeWord"
  }

  /** Cluster description */
  case class TextCluster[T](id: Long) {
    private var _words = ListBuffer[Text]()

    def add(p: Text) {
      _words += p
    }

    def points: Seq[Text] = Seq(_words: _*)

    override def toString() = s"Cluster [$id]\t:\t${_words.size} Text\t${_words.map(_.wholeWord) mkString "|"}"
  }


  /*/** Holding the cluster point */
  case class Point[T](row: Int)(val value: DenseVector[T]) {
    override def toString() = s"[$row]: $value"
  }*/

  /** Cluster description */
  case class Cluster(id: Long) {
    private var _points = ListBuffer[Text]()

    def add(p: Text) {
      _points += p
    }

    def points: Seq[Text] = Seq(_points: _*)

    override def toString() = s"Cluster [$id]\t:\t${_points.size} points\t${_points mkString "|"}"
  }

}

/**
 * Predefined functions for the original DBSCAN algorithm.
 * This can be used like this
 *
 * {{{
 *  val gdbscan = new GDBSCAN(
 *    DBSCAN.getNeighbours(epsilon = 1, distance = Kmeans.euclideanDistance),
 *    DBSCAN.isCorePoint(minPoints = 2)
 *  )
 * }}}
 */
object DBSCAN {

  /**
   * @param epsilon - minimum distance
   */
  def getNeighbours(epsilon: Double, distance: (RoaringBitmap,RoaringBitmap) => Double)(point: Text, points: Seq[Text]): Seq[Text] = {
    points.filter(neighbour => distance(neighbour.vector, point.vector) >= epsilon)
  }

  /**
   * @param minPoints - minimal number of points to be a core point
   */
  def isCorePoint(minPoints: Double)(point: Text, neighbours: Seq[Text]): Boolean = {
    neighbours.size >= minPoints
  }
}