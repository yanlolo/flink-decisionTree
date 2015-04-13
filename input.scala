package lolo

import scala.io.Source
import collection.mutable.ArrayBuffer
import math._
import scala.collection.Map
// map is regarded as immutable by default, if want to use it as a collection, import 

object DecisionTree {
  def main(args: Array[String]): Unit = {
    println("-- Welcom to Decision Tree --")
    println("Input data: ")

    val featureNum = 2 // number of independent features
    val numBins = 5 // B bins for Update procedure
    val numSplit = 3 //By default it should be same as numBins
    var feature = ArrayBuffer[Map[Int, ArrayBuffer[Array[Double]]]]()

    // i is used to distinguish the features
    // j is used to distinguish the labels
    // l is used to distinguish the bins
    
    for (i <- 0 to featureNum - 1) {

      val source = Source.fromFile("d:/Userfiles/yyan/Desktop/data/" + i + ".txt")
      val lines = source.getLines

      var histoList = ArrayBuffer[Array[Double]]() // update result for every labels 
      var labelMap = new collection.mutable.HashMap[Int, ArrayBuffer[Array[Double]]]

      // j is used to distinguish the labels
      var j = 0
      for (line <- lines) {
        val nums = line.toString.split(" ")
        //print input file
        println("line: " + line)
        println("nums size: " + nums.size)

        //update accordingly for every labeled samples
        histoList = updatePro(nums, numBins)
        labelMap += (j -> histoList)
        j += 1
      }
      feature += labelMap
    }

    for (i <- 0 to featureNum - 1) {
      println(" ---- -----feature " + i + "--------- ---- ")
      var labelMap = feature(i)
      for ((k, v) <- labelMap) {
        println(" -----labeled " + k + "--------- ")
        var histolist = v
        for (l <- 0 to numBins - 1)
          println(histolist(l)(0), histolist(l)(1))
      }
    }

  }

  // Update procedure 
  def updatePro(nums: Array[String], numBins: Int): ArrayBuffer[Array[Double]] = {

    var numMerge = 0 // which 2 close bins to merge  
    var histo = ArrayBuffer[Array[Double]]()

    for (i <- 0 to nums.size - 1) {
      if (i < numBins) {
        histo += Array(nums(i).toDouble, 1)
      } else {
        histo += Array(nums(i).toDouble, 1)
        histo = histo.sortWith(_(0) < _(0))

        // Find the closest 2 interval
        var min = Integer.MAX_VALUE.toDouble // interval of 2 bins
        for (j <- 0 to histo.size - 2) {
          if (histo(j + 1)(0).toDouble - histo(j)(0).toDouble < min) {
            min = histo(j + 1)(0).toDouble - histo(j)(0).toDouble
            numMerge = j
          }
        }

        val newBinP = (histo(numMerge)(0) * histo(numMerge)(1) + histo(numMerge + 1)(0) * histo(numMerge + 1)(1)) / (histo(numMerge)(1) + histo(numMerge + 1)(1))
        val newBinK = histo(numMerge)(1) + histo(numMerge + 1)(1)
        val newBin = Array(newBinP, newBinK)

        histo.remove(numMerge + 1)
        histo.remove(numMerge)
        histo.insert(numMerge, newBin)

      }
    }
    histo
  }

}