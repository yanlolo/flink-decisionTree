package lolo

import scala.io.Source
import collection.mutable.ArrayBuffer
import math._
import scala.collection.Map
// map is regarded as immutable by default, if want to use it as a collection, import 
// map could not add same key

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
      println("                    ")
      println(" ---- -----feature " + i + "--------- ---- ")
      var labelMap = feature(i)
      var histo = ArrayBuffer[Array[Double]]() // merge of histoList

      for ((k, v) <- labelMap) {
        println(" -----labeled " + k + "--------- ")
        var histolist = v
        for (l <- 0 to numBins - 1)
          println(histolist(l)(0), histolist(l)(1))

        //sumPro(histolist, 50)(0)
        //uniformPro(histolist, numSplit, 10)
        //histo = mergePro(histo, histolist, numBins)
      }

      //      println(" ---- -----histogram for all samples--------- ---- ")
      //      for (l <- 0 to numBins - 1)
      //        println(histo(l)(0), histo(l)(1))
      //println(entropy(labelMap))
      //println(entropy(labelMap, 50 , 0 ))
    }
    bestFeatureSplit(feature, numBins, numSplit)

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
    var s = 0.0

    if (b >= histo(histo.size - 1)(0)) {
      for (hist <- histo) {
        s += hist(1)
      }
    } else if (b < histo(0)(0)) {
      s = 0
    } else {
      while (b >= histo(i)(0)) {
        i += 1
      }
      i -= 1

      val mi = histo(i)(1)
      val mii = histo(i + 1)(1)
      val pi = histo(i)(0)
      val pii = histo(i + 1)(0)
      val mb = mi + (mii - mi) * (b - pi) / (pii - pi)
      s = (mi + mb) * (b - pi) / (2 * (pii - pi))

      for (j <- 0 to i - 1) {
        s += histo(j)(1)
      }
      s += histo(i)(1) / 2
    }

    //println(" sumPro to " + b + " is " + s)
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
  def uniformPro(histo: ArrayBuffer[Array[Double]], numSplit: Int): Array[Double] = {
    println(" ---- -----candidate for split (interval)--------- ---- ")
    var u = new Array[Double](numSplit - 1)
    var numSample = sumPro(histo)

    for (j <- 1 to numSplit - 1) {
      var s = j * numSample / numSplit.toDouble

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

  //entroy for histogram, the left part and right part
  def entropy(labelMap: Map[Int, ArrayBuffer[Array[Double]]]): Double = {

    var histoSum = 0.0
    var histoOne = new Array[Double](labelMap.size)
    var proSum = 0.0

    var i = 0
    for ((k, v) <- labelMap) {
      histoOne(i) = sumPro(v)
      histoSum += histoOne(i)
      i += 1
    }

    i = 0
    for ((k, v) <- labelMap) {
      if (histoOne(i) == 0) {
        proSum += 0 // there are no samples belong to this label
      } else {
        proSum += -histoOne(i) * log(histoOne(i) / histoSum) / histoSum // log is equal to "ln"
      }
      i += 1
    }
    proSum
  }

  def entropy(labelMap: Map[Int, ArrayBuffer[Array[Double]]], b: Double, flag: Int): Double = {

    var histoOne = new Array[Double](labelMap.size)
    var histoSum = 0.0
    var proSum = 0.0

    var i = 0
    for ((k, v) <- labelMap) {
      if (flag == 0) { //right
        histoOne(i) = sumPro(v, b)(0)
      } else if (flag == 1) { //left
        histoOne(i) = sumPro(v) - sumPro(v, b)(0)
      }
      histoSum += histoOne(i)
      i += 1
    }

    i = 0
    for ((k, v) <- labelMap) {
      // entropy
      if (histoOne(i) == 0) { // there are no samples belong to this label is smaller that b
        proSum += 0
      } else {
        proSum += -histoOne(i) * log(histoOne(i) / histoSum) / histoSum // log is equal to "ln" 
      }
      i += 1
    }
    proSum
  }

  // find the maximum information gain in one feature
  def bestLabelSplit(labelMap: Map[Int, ArrayBuffer[Array[Double]]], numBins: Int, numSplit: Int): Array[Double] = {
    
    var histo = ArrayBuffer[Array[Double]]() // merge of histoList   
    for ((k, v) <- labelMap) {
      histo = mergePro(histo, v, numBins)
    }

    println(" ---- -----histogram for all samples --------- ---- ")
    for (i <- 0 to histo.size - 1) {
      println(histo(i)(0), histo(i)(1))
    }

    var uniform = uniformPro(histo, numSplit)

    var gain = new Array[Double](uniform.size)
    var i = 0
    var maxGain = 0.0
    var maxIndex = 0
    var result = new Array[Double](2)

    

    for (uu <- uniform) {
      var leftPro = sumPro(histo, uu)(0) / sumPro(histo)
      //gain
      gain(i) = entropy(labelMap) - leftPro * entropy(labelMap, uu, 0) - (1 - leftPro) * entropy(labelMap, uu, 1)
      if (gain(i) > maxGain) {
        maxGain = gain(i)
        maxIndex = i
      }
      i += 0
    }
    println("Split at " + uniform(maxIndex)+" the gain is "+maxGain)
    result(0) = uniform(maxIndex)
    result(1) = maxGain //the split place, max gain
    result
  }

  // find the maximum information gain among the features
  def bestFeatureSplit(feature: ArrayBuffer[Map[Int, ArrayBuffer[Array[Double]]]], numBins: Int, numSplit: Int):Array[Double]= {

    var maxGain = 0.0
    var splitplace = 0.0
    var splitFeature = 0
    var aa = new Array[Double](2)
    var result = new Array[Double](2)

    for (i <- 0 to feature.size - 1) {
      aa = bestLabelSplit(feature(i), numBins, numSplit)
      if (aa(1) > maxGain) {
        maxGain = aa(1)
        splitplace = aa(0)
        splitFeature = i
      }
    }

    result(0) = splitFeature
    result(1) = splitplace
    println("Split feature "+splitFeature+" at "+ splitplace)
    result
  }

}