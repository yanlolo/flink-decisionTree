package lolo

import scala.io.Source
import collection.mutable.ArrayBuffer
import math._

object DecisionTree {
  def main(args: Array[String]): Unit = {
    println("--Test for functions--")
    val source = Source.fromFile("d:/Userfiles/yyan/Desktop/data/test.txt")
    val lines = source.getLines

    for (line <- lines) {
      println("---line---  " + line)
      val numStr = line.toString
      println("num string  " + numStr)
      val nums = numStr.split(" ")
      nums.foreach(println)
      println("--nums size--" + nums.size)

      val numBins = 5 // B bins for Update procedure
      var histo = updatePro(nums, numBins)

      // Print out test result
      println(histo(0)(0), histo(0)(1))
      println(histo(1)(0), histo(1)(1))
      println(histo(2)(0), histo(2)(1))
      println(histo(3)(0), histo(3)(1))
      println(histo(4)(0), histo(4)(1))
      val sumTest = sumPro(histo, 15)(0)
      val iTest = sumPro(histo, 15)(1)
      println(sumTest)
      println(iTest)

      var u = uniformPro(histo, 3, nums.size)
      u.foreach(println)

      var a = mergePro(histo, histo, numBins)
      println(a(0)(0), a(0)(1))
      println(a(1)(0), a(1)(1))
      println(a(2)(0), a(2)(1))
      println(a(3)(0), a(3)(1))
      println(a(4)(0), a(4)(1))
      //        println(a(5)(0), a(5)(1))
      //        println(a(6)(0), a(6)(1))
      //        println(a(7)(0), a(7)(1))
      //        println(a(8)(0), a(8)(1))
      //        println(a(9)(0), a(9)(1))
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

  // Sum procedure
  def sumPro(histo: ArrayBuffer[Array[Double]], b: Double): Array[Double] = {
    var i = 0

    while (b >= histo(i)(0)) {
      i += 1
    }
    i -= 1

    val mi = histo(i)(1)
    val mii = histo(i + 1)(1)
    val pi = histo(i)(0)
    val pii = histo(i + 1)(0)
    val mb = mi + (mii - mi) * (b - pi) / (pii - pi)
    var s = (mi + mb) * (b - pi) / (2 * (pii - pi))

    for (j <- 0 to i - 1) {
      s += histo(j)(1)
    }

    s += histo(i)(1) / 2
    val result = Array(s, i)
    result
  }

  // Uniform Procedure
  def uniformPro(histo: ArrayBuffer[Array[Double]], numSplit: Int, numSize: Int): Array[Double] = {
    var u = new Array[Double](numSplit - 1)

    for (j <- 1 to numSplit - 1) {
      var s = j * numSize / numSplit.toDouble

      var i = 0
      var sumP = 0.0
      // Sum Procedure
      while (sumP < s) {
        if (i == 0)
          sumP += histo(i)(1) / 2
        else
          sumP += histo(i)(1) / 2 + histo(i - 1)(1) / 2
        i += 1
      }
      i -= 2

      var d = s - (sumP - histo(i + 1)(1) / 2 - histo(i)(1) / 2)
      var a = histo(i + 1)(1) - histo(i)(1)
      var b = 2 * histo(i)(1)
      var c = -2 * d
      var z = if (a == 0) -c / b else (-b + sqrt(pow(b, 2) - 4 * a * c)) / (2 * a)

      u(j - 1) = histo(i)(0) + (histo(i + 1)(0) - histo(i)(0)) * z
      println("u(" + j + ")=" + u(j - 1))
    }
    u
  }

  // Merge Procedure
  def mergePro(histoA: ArrayBuffer[Array[Double]], histoB: ArrayBuffer[Array[Double]], numBins: Int): ArrayBuffer[Array[Double]] = {
    var histo = ArrayBuffer[Array[Double]]()
    var min = Integer.MAX_VALUE.toDouble // interval of 2 bins

    histo = histoA
    histo ++= histoB //++= is used for collection
    histo = histo.sortWith(_(0) < _(0))

    while (histo.size > numBins) {
      var numMerge = 0

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

    histo

  }

}
