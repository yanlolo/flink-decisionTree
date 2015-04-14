package lolo
//Testing with ....
//23 19 10 16 36 2 9 32 30 45
//46 78 83 30 64 28 87 90 53 72

import scala.io.Source
import collection.mutable.ArrayBuffer
import math._

object DecisionTree {
  def main(args: Array[String]): Unit = {
    println("-- Welcom to Decision Tree --")
    println("Input data: ")
    val source = Source.fromFile("d:/Userfiles/yyan/Desktop/data/test.txt")
    val lines = source.getLines

    var histoList = ArrayBuffer[ArrayBuffer[Array[Double]]]() // update result for every labels 

    val numBins = 5 // B bins for Update procedure
    val numSplit = 3 //By default it should be same as numBins

    for (line <- lines) {
      val nums = line.toString.split(" ")
      //print input file
      println("line: " + line)
      println("nums size: " + nums.size)

      //update accordingly for every labeled samples
      histoList += updatePro(nums, numBins)
    }

    //build one level 
    //buildOneLevel(histoList, numBins, numSplit)

    //buildTree(histoList, numBins, numSplit, 2)

    //    var test = ArrayBuffer[Array[Double]]()
    //    test += Array(2.0, 0.5)
    //    test += Array(9.47, 1.49)
    //    var numSample = 2.0
    //    uniformPro(test, numSplit, numSample)

    var test = ArrayBuffer[ArrayBuffer[Array[Double]]]() // update result for every labels 
    test += ArrayBuffer(Array(27.145006952796034, 2.3192232370864656))
    test += ArrayBuffer(Array(29.0, 2.0), Array(49.5, 2.0), Array(64.0, 1.0), Array(75.0, 2.0), Array(86.7, 3.0))
    buildOneLevel(test, numBins, numSplit)

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
  def uniformPro(histo: ArrayBuffer[Array[Double]], numSplit: Int, numSample: Double): Array[Double] = {
    println(" ---- -----candidate for split (interval)--------- ---- ")
    var u = new Array[Double](numSplit - 1)
    var d = 0.0
    var i = 0

    for (j <- 1 to numSplit - 1) {
      var s = j * numSample / numSplit.toDouble

      if (s <= histo(0)(1)) { // s is too small 
        u(j - 1) = histo(0)(0)
      } else {
        if (s >= numSample - histo(histo.size - 1)(1) / 2) { // s is too large 
          d = s - (numSample - histo(histo.size - 1)(1))
          i = histo.size - 2
        } else {
          i = 0
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
          d = s - (sumP - histo(i + 1)(1) / 2 - histo(i)(1) / 2)
        }

        var a = histo(i + 1)(1) - histo(i)(1)
        var b = 2 * histo(i)(1)
        var c = -2 * d
        var z = if (a == 0) -c / b else (-b + sqrt(pow(b, 2) - 4 * a * c)) / (2 * a)

        u(j - 1) = histo(i)(0) + (histo(i + 1)(0) - histo(i)(0)) * z

      }

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
    println("If feature is smaller than " + uniform(maxIndex))
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

      if (split >= histolist(histolist.size - 1)(0)) { // How if histolist.size = 1 ???!!!
        println(" -- ONLY Left Tree -- ")
        println(j)
        for (ii <- 0 to histolist.size - 1) {
          println(histolist(ii)(0), histolist(ii)(1))
        }
        println(" -- ONLY Left Tree -- ")
        histoLeft(j) = histolist
        //  histoRight += Null                 // modify
        for (ii <- 0 to histolist.size - 1) {
          println(histoLeft(j)(ii)(0), histoLeft(j)(ii)(1))
        }
      } else if (split <= histolist(0)(0)) {
        println(" -- ONLY Right Tree -- ")
        histoRight(j) = histolist
        //    histoLeft += Null                // modify
        for (ii <- 0 to histolist.size - 1) {
          println(ii + " " + j)
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
        if (i > 0) {
          for (ii <- 0 to i - 1) {
            histoleft += histolist(ii)
          }
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
        if (s != 0) {
          for (ii <- 0 to i + 1) {
            println(histoLeft(j)(ii)(0), histoLeft(j)(ii)(1))
          }
        } else {
          for (ii <- 0 to i) {
            println(histoLeft(j)(ii)(0), histoLeft(j)(ii)(1))
          }
        }

        println(" -- Right Tree -- ")
        if (s != 0) {
          for (ii <- 0 to histolist.size - 1 - (i + 1)) {
            println(histoRight(j)(ii)(0), histoRight(j)(ii)(1))
          }
        } else {
          for (ii <- 0 to histolist.size - 1 - i) {
            println(histoRight(j)(ii)(0), histoRight(j)(ii)(1))
          }
        }
      }

      j += 1

    }
    val result = Array(histoLeft, histoRight)
    result
  }

  // which label for samples samller than 'split'
  def label(histoList: ArrayBuffer[ArrayBuffer[Array[Double]]]): Int = {

    var histoOne = new Array[Double](histoList.size)
    var maxHisto = 0.0
    var maxIndex = 0

    var i = 0
    for (histolist <- histoList) {
      if (sumPro(histolist) > maxHisto) {
        maxHisto = sumPro(histolist)
        maxIndex = i
      }
      i += 1
    }
    println("labeled as label " + maxIndex)
    maxIndex
  }

  //    // numbers of all samples
  //    def calcuSampleNum(histoList: ArrayBuffer[ArrayBuffer[Array[Double]]]): Double = {
  //      var num = 0.0
  //      for (histolist <- histoList) {
  //        for (i <- 0 to histolist.size - 1) {
  //          num += histolist(i)(1)
  //        }
  //      }
  //      num
  //    }

  //build up one level of TREE ??==> return!!
  def buildOneLevel(histoList: ArrayBuffer[ArrayBuffer[Array[Double]]], numBins: Int, numSplit: Int): ArrayBuffer[ArrayBuffer[ArrayBuffer[Array[Double]]]] = {

    var tree = ArrayBuffer[ArrayBuffer[ArrayBuffer[Array[Double]]]]()

    // merge all labeled histogram
    var histo = ArrayBuffer[Array[Double]]() // merge of histoList   
    for (histolist <- histoList) {
      histo = mergePro(histo, histolist, numBins)
      println(" ---- -----histogram for labeled samples--------- ---- ")
      for (i <- 0 to histolist.size - 1) {
        println(histolist(i)(0), histolist(i)(1))
      }
    }
    println(" ---- -----histogram for all samples --------- ---- ")
    for (i <- 0 to histo.size - 1) {
      println(histo(i)(0), histo(i)(1))
    }

    //uniform for the histogram
    var numSample = sumPro(histo)
    val uniform = uniformPro(histo, numSplit, numSample)
    var bestSplit = findBestSplit(histoList, uniform, histo)
    var Array(left, right) = split(histoList, bestSplit)
    tree += left
    tree += right
    label(left)
    tree
  }

  //build up one level of TREE
  def buildTree(histoList: ArrayBuffer[ArrayBuffer[Array[Double]]], numBins: Int, numSplit: Int, numLevel: Int) {
    var tree = ArrayBuffer[ArrayBuffer[ArrayBuffer[Array[Double]]]]()
    var tempTree = ArrayBuffer[ArrayBuffer[ArrayBuffer[Array[Double]]]]()

    println("                           ")
    println("            Starting Building      ")
    println("                           ")

    tree = buildOneLevel(histoList, numBins, numSplit)
    for (i <- 1 to numLevel) {
      for (subTree <- tree) { // if subtree != null then split .....!!!
        if (subTree != null) {
          tempTree ++= buildOneLevel(subTree, numBins, numSplit)
        }

      }
      tree = tempTree
      tempTree = ArrayBuffer[ArrayBuffer[ArrayBuffer[Array[Double]]]]()
    }

  }

}