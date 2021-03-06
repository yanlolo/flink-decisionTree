//works for first level split
// show the result for left and right tree


import scala.io.Source
import collection.mutable.ArrayBuffer
import math._

object test {
  def main(args: Array[String]): Unit = {
    println("--Welcom to Decision Tree --")
    println("Input data: ")
    val source = Source.fromFile("d:/Userfiles/yyan/Desktop/data/test.txt")
    val lines = source.getLines

    var histoList = ArrayBuffer[ArrayBuffer[Array[Double]]]() // update result for every labels 

    val numBins = 5 // B bins for Update procedure
    val numSplit = 3 //By default it should be same as numBins
    var numSample = 0 //total number of sample for THE ONLY ONE FEATURE
    // var numSize = ArrayBuffer[Int]() // number of samples in every Label

    for (line <- lines) {
      val nums = line.toString.split(" ")
      //numSize += nums.size
      numSample += nums.size
      //print input file
      println("line: " + line)
      println("nums size: " + nums.size)

      //update accordingly for every labeled samples
      histoList += updatePro(nums, numBins)
    }

    buildTree(histoList, numBins, numSplit, numSample)

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

    println(" sumPro to " + b + " is " + s)
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
  def uniformPro(histo: ArrayBuffer[Array[Double]], numSplit: Int, numSample: Double): Array[Double] = {
    println(" ---- -----candidate for split (interval)--------- ---- ")
    var u = new Array[Double](numSplit - 1)

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
      if (histoOne(i) == 0) { // there are no samples belong to this label is smaller that b
        pro(i) = 0
      } else {
        pro(i) = -histoOne(i) * log(histoOne(i) / histoSum) / histoSum // log is equal to "ln" 
      }
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
      //println("histoSum"+histoSum)
      i += 1
    }

    i = 0
    for (histolist <- histoList) {
      // entropy
      if (histoOne(i) == 0) { // there are no samples belong to this label is smaller that b
        pro(i) = 0
      } else {
        pro(i) = -histoOne(i) * log(histoOne(i) / histoSum) / histoSum // log is equal to "ln" 
      }
      //println(pro(i))
      proSum += pro(i)
      i += 1
    }
    proSum
  }

  // find the maximum information gain 
  def findBestSplit(histoList: ArrayBuffer[ArrayBuffer[Array[Double]]], uniform: Array[Double], histo: ArrayBuffer[Array[Double]]): Double = {

    var gain = new Array[Double](uniform.size)
    var i = 0
    var maxGain = 0.0
    var maxIndex = 0

    for (uu <- uniform) {
      var leftPro = sumPro(histo, uu)(0) / sumPro(histo)
      //gain
      gain(i) = entropy(histoList) - leftPro * entropy(histoList, uu, 0) - (1 - leftPro) * entropy(histoList, uu, 1)
      if (gain(i) > maxGain) {
        maxGain = gain(i)
        maxIndex = i
      }
      i += 0
    }
    uniform(maxIndex) //the split place
  }

  // split the samples into 2 parts based on the "gain"
  def split(histoList: ArrayBuffer[ArrayBuffer[Array[Double]]], split: Double): Array[ArrayBuffer[ArrayBuffer[Array[Double]]]] = {
    var histoLeft = ArrayBuffer[ArrayBuffer[Array[Double]]]()
    var histoRight = ArrayBuffer[ArrayBuffer[Array[Double]]]()

    var j = 0

    for (histolist <- histoList) {

      var histoleft = ArrayBuffer[Array[Double]]()
      var historight = ArrayBuffer[Array[Double]]()
      var i = 0

      if (split >= histolist(histolist.size - 1)(0)) {
        histoLeft += histolist
        println(" -- ONLY Left Tree -- ")
        for (ii <- 0 to histolist.size - 1) {
          println(histoLeft(j)(ii)(0), histoLeft(j)(ii)(1))
        }
      } else if (split < histolist(0)(0)) {
        histoRight += histolist
        println(" -- ONLY Right Tree -- ")
        for (ii <- 0 to histolist.size - 1) {
          println(histoRight(j)(ii)(0), histoRight(j)(ii)(1))
        }
      } else {
        while (split >= histolist(i)(0)) {
          i += 1
        }
        i -= 1

        val mi = histolist(i)(1)
        val mii = histolist(i + 1)(1)
        val pi = histolist(i)(0)
        val pii = histolist(i + 1)(0)
        val mb = mi + (mii - mi) * (split - pi) / (pii - pi)
        var s = (mi + mb) * (split - pi) / (2 * (pii - pi))

        //left subtree
        for (ii <- 0 to i - 1) {
          histoleft += histolist(ii)
        }
        histoleft += Array(histolist(i)(0), histolist(i)(1) / 2)
        if (s != 0) {
          histoleft += Array(split, s)
        }
        histoLeft += histoleft

        //right subtree
        historight += Array(split, histolist(i + 1)(1) + histolist(i)(1) / 2 - s) //wenti !!!!
        for (ii <- i + 2 to histolist.size - 1) {
          historight += histolist(ii)
        }
        histoRight += historight

        println(" -- Left Tree -- ")
        for (ii <- 0 to i + 1) {
          println(histoLeft(j)(ii)(0), histoLeft(j)(ii)(1))
        }

        println(" -- Right Tree -- ")
        for (ii <- 0 to histolist.size - 1 - (i + 1)) {
          println(histoRight(j)(ii)(0), histoRight(j)(ii)(1))
        }

      }

      j += 1

    }
    val result = Array(histoLeft, histoRight)
    result
  }

  //build up the tree
  def buildTree(histoList: ArrayBuffer[ArrayBuffer[Array[Double]]], numBins: Int, numSplit: Int, numSample: Double) {
    println("                           ")
    println("            Starting Building      ")
    println("                           ")

    var histo = ArrayBuffer[Array[Double]]() // merge of histoList
    // merge all labeled histogram
    for (histolist <- histoList) {
      histo = mergePro(histo, histolist, numBins)
      println(" ---- -----histogram for labeled samples--------- ---- ")
      println(histolist(0)(0), histolist(0)(1))
      println(histolist(1)(0), histolist(1)(1))
      println(histolist(2)(0), histolist(2)(1))
      println(histolist(3)(0), histolist(3)(1))
      println(histolist(4)(0), histolist(4)(1))
    }
    println(" ---- -----histogram for all samples--------- ---- ")
    println(histo(0)(0), histo(0)(1))
    println(histo(1)(0), histo(1)(1))
    println(histo(2)(0), histo(2)(1))
    println(histo(3)(0), histo(3)(1))
    println(histo(4)(0), histo(4)(1))

    //uniform for the histogram
    val uniform = uniformPro(histo, numSplit, numSample)

    //    // Test for sumPro
    //    for (histolist <- histoList) {
    //      println(" ---- -----labeled--------- ---- ")
    //      println(histolist(0)(0), histolist(0)(1))
    //      println(histolist(1)(0), histolist(1)(1))
    //      println(histolist(2)(0), histolist(2)(1))
    //      println(histolist(3)(0), histolist(3)(1))
    //      println(histolist(4)(0), histolist(4)(1))
    //      sumPro(histolist, 100)
    //    }

    //    // Test for entropy, findBestSplit
    //    println("entrpy=" + entropy(histoList))
    //    println("entrpy=" + entropy(histoList, 25.91, 1))
    var bestSplit = findBestSplit(histoList, uniform, histo)
    println("Split at " + bestSplit)

    split(histoList, bestSplit)
  }

}