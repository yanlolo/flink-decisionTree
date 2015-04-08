package lolo

import scala.io.Source
import collection.mutable.ArrayBuffer
import math._

object DecisionTree {
  def main(args: Array[String]): Unit = {
    println("--Decision Tree--")
    val source = Source.fromFile("d:/Userfiles/yyan/Desktop/data/test.txt")
    val lines = source.getLines

    var histoList = ArrayBuffer[ArrayBuffer[Array[Double]]]() // update result for every labels 
    var histo = ArrayBuffer[Array[Double]]() // merge of histoList
    val numBins = 5 // B bins for Update procedure
    val numSplit = 3 //By default it should be same as numBins
    var numSizeTotal = 0 //total number of sample for THE ONLY ONE FEATURE
    var numSize = ArrayBuffer[Int]() // number of samples in every Label

    for (line <- lines) {
      val nums = line.toString.split(" ")
      numSize += nums.size
      numSizeTotal += nums.size
      //print input file
      println("---line---  " + line)
      println("--nums size--" + nums.size)

      //update accordingly for every labeled samples
      histoList += updatePro(nums, numBins)
    }

    // merge all labeled histogram
    for (histolist <- histoList) {
      histo = mergePro(histo, histolist, numBins)
    } //print all sample's histogram
    println(histo(0)(0), histo(0)(1))
    println(histo(1)(0), histo(1)(1))
    println(histo(2)(0), histo(2)(1))
    println(histo(3)(0), histo(3)(1))
    println(histo(4)(0), histo(4)(1))

    //uniform for the histogram
    val uniform = uniformPro(histo, numSplit, numSizeTotal)

    //entropy, gain of the split
    println("entrpy="+entropy(histoList))
    println("gain="+gain(histoList, uniform, histo))

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

  def sumPro(histo: ArrayBuffer[Array[Double]]): Double = {
    var result = 0.0
    for (hist <- histo) {
      result += hist(1)
    }
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

  def entropy(histoList: ArrayBuffer[ArrayBuffer[Array[Double]]]): Double = {

    var histoOne = new Array[Double](histoList.size)
    var histoSum = 0.0
    var pro = new Array[Double](histoList.size)
    var proSum = 0.0

    var i = 0
    for (histolist <- histoList) {
      histoOne(i) = sumPro(histolist)
      histoSum += histoOne(i)
      i += 1
    }

    i = 0
    for (histolist <- histoList) {
      // entropy
      pro(i) = -histoOne(i) * log(histoOne(i) / histoSum) / histoSum // log is equal to "ln" 
      proSum += pro(i)
      i += 1
    }
    proSum
  }

  def entropy(histoList: ArrayBuffer[ArrayBuffer[Array[Double]]], b: Double, flag: Int): Double = {

    var histoOne = new Array[Double](histoList.size)
    var histoSum = 0.0
    var pro = new Array[Double](histoList.size)
    var proSum = 0.0

    var i = 0
    for (histolist <- histoList) {
      if (flag == 0) { //right
        histoOne(i) = sumPro(histolist, b)(0)
      } else if (flag == 1) { //left
        histoOne(i) = sumPro(histolist) - sumPro(histolist, b)(0)
      }

      histoSum += histoOne(i)
      i += 1
    }

    i = 0
    for (histolist <- histoList) {
      // entropy
      pro(i) = -histoOne(i) * log(histoOne(i) / histoSum) / histoSum // log is equal to "ln" 
      proSum += pro(i)
      i += 1
    }
    proSum
  }

  def gain(histoList: ArrayBuffer[ArrayBuffer[Array[Double]]], uniform: Array[Double], histo: ArrayBuffer[Array[Double]]): Int = {

    var gain = new Array[Double](uniform.size)
    var i = 0
    var maxGain = 0.0
    var maxIndex = 0

    for (uu <- uniform) {
      var leftPro = sumPro(histo, uu)(0) / sumPro(histo)
      gain(i) = entropy(histoList) - leftPro * entropy(histoList, uu, 0) - (1 - leftPro) * entropy(histoList, uu, 1)
      if (gain(i) > maxGain) {
        maxGain = gain(i)
        maxIndex = i
      }
      i += 0
    }
    maxIndex
  }

}

