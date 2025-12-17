package com.track.mining.trackmanager

import java.io.{File, PrintWriter}

import com.track.mining.trackmanager.bean.LatLng
import com.track.mining.trackmanager.service.PointService

import scala.io.Source
import scala.util.matching.Regex

/**
  * Visualization capabilities for trajectory data results in this category
  */
object TrackView {

  /**
    * Replace the latitude and longitude coordinates in the HTML file
    * @param filePath HTML file path
    * @param newCoordinates New latitude and longitude array, formatted as Array(Array(lat, lng), ...)
    * @param outputPath Output file path
    */
  def replaceCoordinates(filePath: String, newCoordinates: Array[LatLng], outputPath: String): Unit = {

    // Read the contents of the template file
    val content = Source.fromFile(filePath, "UTF-8").mkString

    //Calculate the average distance
    val newCenter = PointService.calculateCentroid(newCoordinates)
    // Replace Center Point Coordinates
    var updatedContent = replaceCenterCoordinate(content, newCenter)
    // Replace Scatter Plot
    updatedContent = replacePolylineCoordinates(updatedContent, newCoordinates)
    // Replace marker point coordinates
    val finalContent = replaceMarkerCoordinates(updatedContent, newCoordinates)

    // Write to a new file
    val writer = new PrintWriter(new File(outputPath), "UTF-8")
    writer.write(finalContent)
    writer.close()

    println(s"Coordinate replacement complete! New file saved to: $outputPath")
    println(s"A total of ${newCoordinates.length} coordinate points")
  }

  /**
    * Replace Center Point Coordinates
    */
  private def replaceCenterCoordinate(content: String, center: (Double,Double)): String = {
    val centerPattern = """center:\s*\[([\d.]+),\s*([\d.]+)\]""".r

    centerPattern.replaceAllIn(content, m => {
      s"center: [${center._1}, ${center._2}]"
    })
  }

  /**
    * replacePolylineCoordinates
    */
  private def replacePolylineCoordinates(content: String, coordinates: Array[LatLng]): String = {
    // Construct a new coordinate array string
    val newCoordsString = coordinates.map(coord => s"[${coord.latitude}, ${coord.longitude}]").mkString("[", ", ", "]")

    // Match and Replace Scatter Coordinates
    val polylinePattern = """L\.polyline\(\s*\[\[.*?\]\]""".r
    polylinePattern.replaceFirstIn(content, s"L.polyline($newCoordsString")
  }

  /**
    * Replace the coordinates of all marked points
    */
  private def replaceMarkerCoordinates(content: String, coordinates: Array[LatLng]): String = {
    var updatedContent = content

    // Pattern matching all marked point coordinates
    val markerPattern = """L\.marker\(\s*\[(\d+\.\d+),\s*(\d+\.\d+)\]""".r

    // Find all matching coordinate positions
    val matches = markerPattern.findAllMatchIn(content).toArray

    if (matches.length != coordinates.length) {
      println(s"Warning: The number of new coordinates (${coordinates.length}) does not match the number of markers in the file (${matches.length}).")
    }

    // Replace each marker point in sequence
    for (i <- coordinates.indices) {
      if (i < matches.length) {
        val matchItem = matches(i)
        val oldCoord = s"[${matchItem.group(1)}, ${matchItem.group(2)}]"
        val newCoord = s"[${coordinates(i).latitude}, ${coordinates(i).longitude}]"

        // Replace the first match
        updatedContent = updatedContent.replaceFirst(
          java.util.regex.Pattern.quote(oldCoord),
          newCoord
        )
      }
    }
    // Then add new marker points.
    updatedContent = addNewMarkers(updatedContent, coordinates, matches.length)
    updatedContent
  }

  /**
    * Add a new marker to the map
    */
  private def addNewMarkers(content: String, coordinates: Array[LatLng], startIndex: Int): String = {
    // Locate the reference positions of the marked point clusters
    val markerClusterPattern = """var marker_cluster_[a-f0-9]+ = L\.markerClusterGroup\([^;]+;""".r
    val markerClusterMatch = markerClusterPattern.findFirstIn(content)

    // Locate the code section for adding marked point clusters to the map.
    val addToMapPattern = """map_[a-f0-9]+\.addLayer\(marker_cluster_[a-f0-9]+\);""".r
    val addToMapMatch = addToMapPattern.findFirstIn(content)

    if (markerClusterMatch.isEmpty || addToMapMatch.isEmpty) {
      println("Warning: The marker point cluster definition could not be found. New marker points will not be added to the cluster！！！")
      return content
    }

    // Build new marker point code
    val newMarkersCode = new StringBuilder

    for (i <- startIndex until coordinates.length) {
      val lat = coordinates(i).latitude
      val lng = coordinates(i).longitude

      // Generate a random ID to ensure uniqueness
      val markerId = generateRandomId()
      val popupId = generateRandomId()
      val htmlId = generateRandomId()

      newMarkersCode ++= s"""
                            |
            |            var marker_$markerId = L.marker(
                            |                [$lat, $lng],
                            |                {"color": "#037ef3", "opacity": 0.6}
                            |            ).addTo(marker_cluster_77c58986b273149acf4af3236dd026b3);
                            |
                            |
                            |        var popup_$popupId = L.popup({"maxWidth": "100%"});
                            |
            |
                            |
                            |                var html_$htmlId = $$(`<div id="html_$htmlId" style="width: 100.0%; height: 100.0%;"><b>$lat,$lng</b></div>`)[0];
                            |                popup_$popupId.setContent(html_$htmlId);
                            |
                            |
                            |
            |        marker_$markerId.bindPopup(popup_$popupId)
                            |        ;
                            |
            |
                            |""".stripMargin
    }

    newMarkersCode.append(generateStartOrEndPoint("Start Point",coordinates.head))
    newMarkersCode.append(generateStartOrEndPoint("End Point",coordinates.last))
    // Insert a new marker before the code that adds the marker point cluster to the map.
    content.replace(addToMapMatch.get, newMarkersCode.toString() + "\n" + addToMapMatch.get)
  }

  /**
    * Generate a random ID
    */
  private def generateRandomId(): String = {
    java.util.UUID.randomUUID().toString.replace("-", "").substring(0, 16)
  }

  /**
    *
    * @param pointType  Point types: Start Point, End Point
    * @param point      Latitude and longitude
    * @return Return the information that needs to be supplemented.
    */
  private def generateStartOrEndPoint(pointType:String, point:LatLng):String = {
    // Generate a random ID to ensure uniqueness
    val markerId = generateRandomId()
    val popupId = generateRandomId()
    val htmlId = generateRandomId()
    val iconId = generateRandomId()
    val color = pointType match {
      case "Start Point" => "red"
      case "End Point" => "green"
    }
    val startPointCode = s"""
                            |
            |            var marker_$markerId = L.marker(
                            |                [${point.latitude}, ${point.longitude}],
                            |                {"zIndexOffset": 999}
                            |            ).addTo(map_a05d830bdb333dcc4a4ecb44893e0ac4);
                            |
                            |
                            |            var icon_$iconId = L.AwesomeMarkers.icon(
                            |                {"extraClasses": "fa-rotate-0", "icon": "cloud", "iconColor": "white", "markerColor": "${color}", "prefix": "glyphicon"}
                            |            );
                            |            marker_$markerId.setIcon(icon_$iconId);
                            |
                            |
                            |        var popup_$popupId = L.popup({"maxWidth": "100%"});
                            |
            |
                            |
                            |                var html_$htmlId = $$(`<div id="html_$htmlId" style="width: 100.0%; height: 100.0%;"><b>$pointType</b></div>`)[0];
                            |                popup_$popupId.setContent(html_$htmlId);
                            |
                            |
                            |
            |        marker_$markerId.bindPopup(popup_$popupId)
                            |        ;
                            |
            |
                            |""".stripMargin
    startPointCode
  }
}