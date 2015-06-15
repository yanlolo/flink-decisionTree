package org.myorg.quickstart

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.functions._
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConverters._
import java.lang.Iterable
import math._
import scala.collection.mutable.ArrayBuffer

object WordCount {

  def main(args: Array[String]) {

    if (!parseParameters(args)) {
      return
    }

    // get execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get data set
    val input = getDataSet(env)
    val datasets: DataSet[String] = input.flatMap { _.split("\n") } filter { _.nonEmpty }
    val nonEmptyDatasets: DataSet[Array[String]] = datasets.map { s => s.split("\t") }.filter { !_.contains("") }
    //nonEmptyDatasets.map { s => s.toList }.writeAsText("/home/hadoop/Desktop/test/nonEmptyDatasets")

    val labledSample: DataSet[LabeledVector] = inputPro(nonEmptyDatasets)
    //labledSample.map { s => (s.position, s.label, s.feature.toList) }.writeAsText("/home/hadoop/Desktop/test/labledSample")
    val totalNum = labledSample.map { s => 1 }.reduce { _ + _ }

    var result = partition(labledSample)
    var inputTo: DataSet[LabeledVector] = result._1
    var split: DataSet[(String, Int, Double, Double)] = result._2

    //    for (i <- 1 to numLevel - 1) {
    //      result = partition(inputTo)
    //      inputTo = result._1
    //      split = split.union(result._2)
    //    }

    inputTo.map { s => (s.position, s.label, s.feature.toList) }.writeAsText("/home/hadoop/Desktop/test/splitedData")
    split.writeAsText("/home/hadoop/Desktop/test/split")

    val testErr = testErrCal(inputTo, totalNum)
    testErr.writeAsText("/home/hadoop/Desktop/test/testErr")

    val labledCateSample: DataSet[LabeledVectorStr] = inputProCate(nonEmptyDatasets)
    val splitedCateSample: DataSet[LabeledVectorStr] = partitionCate(labledCateSample)
    splitedCateSample.map { s => (s.position, s.label, s.feature.toList) } writeAsText ("/home/hadoop/Desktop/test/splitedCateSample")

    // execute program
    env.execute(" Decision Tree ")
  }

  // *************************************************************************
  //  UTIL Variable
  // *************************************************************************  
  private var inputPath: String = null
  private var outputPath: String = null
  private val numFeature = 2 // number of independent features
  private val numBins = 5 // B bins for Update procedure
  private val numSplit = 3 //By default it should be same as numBins
  private val numLevel = 3 // how many levels of tree
  private val leastSample = 5 // least number of samples in one node

  case class LabeledVector(position: String, label: Double, feature: Array[Double])
  case class Histo(featureValue: Double, frequency: Double)
  case class Histogram(position: String, label: Double, featureIndex: Int, histo: Array[Histo])
  case class MergedHisto(position: String, featureIndex: Int, histo: Array[Histo])
  case class NumSample(position: String, number: Int)
  case class Uniform(position: String, featureIndex: Int, uniform: Array[Double])
  case class Sum(position: String, label: Double, featureIndex: Int, sum: Array[Double])
  case class Gain(position: String, featureIndex: Int, gain: Array[Double])
  case class Frequency(position: String, label: Double, frequency: Double)

  case class LabeledVectorStr(position: String, label: Double, feature: Array[String])
  case class HistogramStr(position: String, label: Double, featureIndex: Int, featureValue: String, frequency: Double)
  case class GainStr(position: String, featureIndex: Int, featureValue: String, gain: Double)

  // *************************************************************************
  //  UTIL METHODS
  // *************************************************************************

  private def parseParameters(args: Array[String]): Boolean = {
    println(" start parse")
    if (args.length == 2) {

      inputPath = args(0)
      outputPath = args(1)
      println(" stop parse")
      true
    } else {
      System.err.println("Please set input/output path. \n")
      false
    }
  }

  private def getDataSet(env: ExecutionEnvironment): DataSet[String] = {
    println(" start input")
    env.readTextFile(inputPath)
  }

  /*
   * input data process
   */
  def inputPro(nonEmptyDatasets: DataSet[Array[String]]): DataSet[LabeledVector] = {

    val nonEmptySample: DataSet[Array[Double]] = nonEmptyDatasets.map { s => s.take(14)
    }.map { s =>
      //var re = new Array[Double](3)
      var re = new Array[Double](14)
      var i = 0

      for (aa <- s) {
        if (i <= 13)
          re(i) = aa.toDouble
        i += 1
      }
      re
    }

    val labledSample: DataSet[LabeledVector] = nonEmptySample.map { s =>
      new LabeledVector("", s(0), s.drop(1).take(13))
    }

    labledSample
  }

  /*
   * update process
   */
  def updatePro(h1: Histogram, h2: Histogram): Histogram = {
    var re = new Histogram(h1.position, 0, 0, null)
    var h = (h1.histo ++ h2.histo).sortBy(_.featureValue) //accend

    if (h.size <= numBins) {
      re = new Histogram(h1.position, h1.label, h1.featureIndex, h)
    } else {
      while (h.size > numBins) {
        var minIndex = 0
        var minValue = Integer.MAX_VALUE.toDouble
        for (i <- 0 to h.size - 2) {
          if (h(i + 1).featureValue - h(i).featureValue < minValue) {
            minIndex = i
            minValue = h(i + 1).featureValue - h(i).featureValue
          }
        }
        val newfrequent = h(minIndex).frequency + h(minIndex + 1).frequency
        val newValue = (h(minIndex).featureValue * h(minIndex).frequency + h(minIndex + 1).featureValue * h(minIndex + 1).frequency) / newfrequent
        val newFea = h.take(minIndex) ++ Array(Histo(newValue, newfrequent)) ++ h.drop(minIndex + 2)
        h = newFea
      }
      re = new Histogram(h1.position, h1.label, h1.featureIndex, h)
    }
    re
  }

  /*
   * merge process
   */
  def mergePro(m1: MergedHisto, m2: MergedHisto): MergedHisto = {
    var re = new MergedHisto(m1.position, 0, null)
    var h = (m1.histo ++ m2.histo).sortBy(_.featureValue) //accend
    if (h.size <= numBins) {
      re = new MergedHisto(m1.position, m1.featureIndex, h)
    } else {
      while (h.size > numBins) {
        var minIndex = 0
        var minValue = Integer.MAX_VALUE.toDouble
        for (i <- 0 to h.size - 2) {
          if (h(i + 1).featureValue - h(i).featureValue < minValue) {
            minIndex = i
            minValue = h(i + 1).featureValue - h(i).featureValue
          }
        }
        val newfrequent = h(minIndex).frequency + h(minIndex + 1).frequency
        val newValue = (h(minIndex).featureValue * h(minIndex).frequency + h(minIndex + 1).featureValue * h(minIndex + 1).frequency) / newfrequent
        val newFea = h.take(minIndex) ++ Array(Histo(newValue, newfrequent)) ++ h.drop(minIndex + 2)
        h = newFea
      }
      re = new MergedHisto(m1.position, m1.featureIndex, h)
    }
    re
  }

  /*
   * uniform process
   */
  def uniformPro(sample: (NumSample, MergedHisto)): Uniform = {
    val histo = sample._2.histo //ascend
    val len = histo.length
    var u = new Array[Double](numSplit - 1)

    for (j <- 1 to numSplit - 1) {
      var s = j * sample._1.number / numSplit.toDouble

      if (s <= histo(0).frequency) {
        u(j - 1) = histo(0).featureValue
      } else {
        val totalSum = (0 until len) map { index => histo(index).frequency } reduce { _ + _ }
        val extendSum = totalSum - histo(len - 1).frequency / 2
        if (s >= extendSum) {
          u(j - 1) = histo(len - 1).featureValue
        } else {
          var i = 0
          var sumP = 0.0
          //sumPro
          while (sumP < s) {
            if (i == 0)
              sumP += histo(i).frequency / 2
            else
              sumP += histo(i).frequency / 2 + histo(i - 1).frequency / 2
            i += 1
          }
          i -= 2

          var d = s - (sumP - histo(i + 1).frequency / 2 - histo(i).frequency / 2)
          var a = histo(i + 1).frequency - histo(i).frequency
          var b = 2 * histo(i).frequency
          var c = -2 * d
          var z = if (a == 0) -c / b else (-b + sqrt(pow(b, 2) - 4 * a * c)) / (2 * a)

          u(j - 1) = histo(i).featureValue + (histo(i + 1).featureValue - histo(i).featureValue) * z
        }
      }
    }
    new Uniform(sample._1.position, sample._2.featureIndex, u)
  }

  /*
   * sum process
   */
  def sumPro(uni: Uniform, updatedSample: Histogram): Sum = {

    val histo = updatedSample.histo
    val len = histo.length
    var s = new Array[Double](uni.uniform.length)

    var k = 0
    for (b <- uni.uniform) {
      var i = 0

      if (len == 0) {
        s(k) = 0.0
      } else if (b <= histo(0).featureValue) {
        s(k) = 0.0
      } else if (b >= histo(len - 1).featureValue) {
        s(k) = (0 until len) map { index => histo(index).frequency } reduce { _ + _ }
      } else {
        while (b >= histo(i).featureValue) {
          i += 1
        }
        i -= 1

        val mi = histo(i).frequency
        val mii = histo(i + 1).frequency
        val pi = histo(i).featureValue
        val pii = histo(i + 1).featureValue
        val mb = mi + (mii - mi) * (b - pi) / (pii - pi)
        s(k) = (mi + mb) * (b - pi) / (2 * (pii - pi))

        for (j <- 0 to i - 1) {
          s(k) += histo(j).frequency
        }
        s(k) += histo(i).frequency / 2

      }
      k += 1
    }
    new Sum(updatedSample.position, updatedSample.label, updatedSample.featureIndex, s)
  }

  // *************************************************************************
  //  GAIN
  // *************************************************************************

  /*
   * calculate the left part entropy
   */
  def entropyLeftCal(sum: DataSet[Sum]): DataSet[Sum] = {

    //frequency for every "position", "featureIndex", regardless of "label"
    val entropy: DataSet[Sum] = sum.groupBy("position", "featureIndex") reduce {
      (h1, h2) => new Sum(h1.position, 0, h1.featureIndex, h1.sum.zipWithIndex.map { case (e, i) => e + h2.sum(i) })
    }

    // Proportion of every label's frequency for "entropy"
    val entropy2: DataSet[Sum] = sum.join(entropy).where("position", "featureIndex").equalTo("position", "featureIndex")
      .map { s =>
        new Sum(s._1.position, s._1.label, s._1.featureIndex, s._1.sum.zipWithIndex.map {
          case (e, i) =>
            var re = 0.0
            if (s._2.sum(i) == 0) {
              re = 0.0
            } else {
              re = e / s._2.sum(i)
            }
            re
        })
      }

    // sum up the proportion for every label 
    val entropy3: DataSet[Sum] = entropy2.groupBy("position", "featureIndex") reduce {
      (h1, h2) =>
        new Sum(h1.position, 0, h1.featureIndex, h1.sum.zipWithIndex.map {
          case (e, i) =>
            var a = 0.0
            if (e > 0) {
              a = -e * log(e)
            }

            var b = 0.0
            if (h2.sum(i) > 0) {
              b = -h2.sum(i) * log(h2.sum(i))
            }
            a + b
        })
    }

    entropy3
  }

  /*
   * calculate the entropy of the dataset 
   */
  def entropyCal(numSampleByLabel: DataSet[Frequency]): DataSet[Frequency] = {

    //frequency for every "position" of different "label"
    val entropy: DataSet[Frequency] = numSampleByLabel.groupBy("position").reduce {
      (h1, h2) => new Frequency(h1.position, 0, h1.frequency + h2.frequency)
    }

    // Proportion of every label's frequency 
    val entropy2: DataSet[Frequency] = numSampleByLabel.join(entropy).where("position").equalTo("position")
      .map {
        s => new Frequency(s._1.position, s._1.label, s._1.frequency / s._2.frequency)
      }

    // sum up 
    val entropy3: DataSet[Frequency] = entropy2.groupBy("position").reduce {
      (h1, h2) =>
        {
          var a = 0.0
          if (h1.frequency > 0) {
            a = -h1.frequency * log(h1.frequency)
          }

          var b = 0.0
          if (h2.frequency > 0) {
            b = -h2.frequency * log(h2.frequency)
          }
          new Frequency(h1.position, 0, a + b)
        }
    }
    entropy3
  }

  /*
     *  calculate gain for every split candidates
     */
  def gainCal(labledSample: DataSet[LabeledVector], sum: DataSet[Sum]): DataSet[Gain] = {
    val numSampleByLabel: DataSet[Frequency] = labledSample.map { s => new Frequency(s.position, s.label, 1) }
      .groupBy("position", "label").sum("frequency")
    //numSampleByLabel.writeAsText("/home/hadoop/Desktop/test/numSampleByLabel")

    val entropy: DataSet[Frequency] = entropyCal(numSampleByLabel)
    //entropy.writeAsText("/home/hadoop/Desktop/test/entropy")

    val entropyLeft: DataSet[Sum] = entropyLeftCal(sum)
    //entropyLeft.map { s => (s.position, s.label, s.featureIndex, s.sum.toList) } writeAsText ("/home/hadoop/Desktop/test/entropyLeft")

    val sumRight: DataSet[Sum] = sum.join(numSampleByLabel).where("position", "label").equalTo("position", "label") {
      (sum, numSampleByLabel, out: Collector[Sum]) =>
        out.collect(new Sum(sum.position, sum.label, sum.featureIndex, sum.sum.zipWithIndex.map { case (e, i) => numSampleByLabel.frequency - e }))
    }
    val entropyRight: DataSet[Sum] = entropyLeftCal(sumRight)
    //entropyRight.map { s => (s.position, s.label, s.featureIndex, s.sum.toList) } writeAsText ("/home/hadoop/Desktop/test/entropyRight")

    val delta1: DataSet[Sum] = sum.groupBy("position", "featureIndex") reduce {
      (h1, h2) => new Sum(h1.position, 0, h1.featureIndex, h1.sum.zipWithIndex.map { case (e, i) => e + h2.sum(i) })
    }
    //delta1.map { s => (s.position, s.label, s.featureIndex, s.sum.toList) } writeAsText ("/home/hadoop/Desktop/test/delta1")

    val totalSample = labledSample.map { s => (s.position, 1) }.groupBy(0).reduce((s1, s2) => (s1._1, s1._2 + s2._2))
    //totalSample.map { s => (s._1, s._2) } writeAsText ("/home/hadoop/Desktop/test/totalSample")

    val delta: DataSet[Gain] = delta1.join(totalSample).where("position").equalTo(0)
      .map {
        s => new Gain(s._1.position, s._1.featureIndex, s._1.sum.zipWithIndex.map { case (e, i) => e / s._2._2 })
      }
    //delta.map { s => (s.position, s.featureIndex, s.gain.toList) }.writeAsText("/home/hadoop/Desktop/test/delta")

    val gain: DataSet[Gain] = entropyLeft.join(entropyRight).where("position", "label", "featureIndex").equalTo("position", "label", "featureIndex")
      .map { s => (s._1.position, s._1.featureIndex, s._1.sum, s._2.sum) }
      .join(delta).where(0, 1).equalTo("position", "featureIndex")
      .map { s => (s._1._1, s._1._2, s._1._3, s._1._4, s._2.gain) }
      .join(entropy).where(0).equalTo("position")
      //(1position, 2featureIndex, 3entropyLeft, 4entropyRight, 5delta, 6entropy)
      .map { s => (s._1._1, s._1._2, s._1._3, s._1._4, s._1._5, s._2.frequency) }
      .map { s =>
        new Gain(s._1, s._2, s._3.zipWithIndex.map {
          case (e, i) => s._6 - s._5(i) * e - (1 - s._5(i)) * s._4(i)
        })
      }
    gain.map { s => (s.position, s.featureIndex, s.gain.toList) } writeAsText ("/home/hadoop/Desktop/test/gain")

    gain
  }

  /*
   * find the split place
   */
  def findSplitPlace(gain: DataSet[Gain], uniform: DataSet[Uniform]): DataSet[(String, Int, Double, Double)] = {
    // (the split feature, the uniform place)
    val splitPlace1 = gain.map {
      s =>
        val feature = s.gain
        var max = Integer.MIN_VALUE.toDouble
        var maxIndex = 0
        for (i <- 0 until feature.length) {
          if (feature(i) > max) {
            max = feature(i)
            maxIndex = i
          }
        }
        (s.position, s.featureIndex, maxIndex, max)
    }.groupBy(0).reduce { (s1, s2) =>
      var re = (s1._1, 0, 0, 0.0)
      if (s1._4 <= s2._4)
        re = (s1._1, s2._2, s2._3, s2._4)
      else
        re = (s1._1, s1._2, s1._3, s1._4)
      re
    }

    val splitPlace = splitPlace1.join(uniform).where(0).equalTo("position")
      .filter { s => (s._2.featureIndex == s._1._2) } // get the matched feature
      .map {
        s => (s._1._1, s._1._2, s._2.uniform(s._1._3), s._1._4)
      }
    splitPlace.writeAsText("/home/hadoop/Desktop/test/splitPlaceTest")

    splitPlace
  }

  /*
   * partition one level
   */
  def partition(labledSample: DataSet[LabeledVector]): (DataSet[LabeledVector], DataSet[(String, Int, Double, Double)]) = {
    val histoSample: DataSet[Histogram] = labledSample.flatMap { s =>
      (0 until s.feature.size) map {
        index => new Histogram(s.position, s.label, index, Array(Histo(s.feature(index), 1)))
      }
    }
    //histoSample.map { s => (s.position, s.label, s.featureIndex, s.histo.toList) }.writeAsText("/home/hadoop/Desktop/test/histoSample")

    val updatedSample: DataSet[Histogram] = histoSample.groupBy("position", "label", "featureIndex") reduce {
      (h1, h2) => updatePro(h1, h2)
    }
    //updatedSample.map { s => (s.position, s.label, s.featureIndex, s.histo.toList) }.writeAsText("/home/hadoop/Desktop/test/updatedSample")

    val mergedSample: DataSet[MergedHisto] = updatedSample.map { s => new MergedHisto(s.position, s.featureIndex, s.histo) }
      .groupBy("position", "featureIndex") reduce {
        (m1, m2) => mergePro(m1, m2)
      }
    //mergedSample.map { s => (s.position, s.featureIndex, s.histo.toList) }.writeAsText("/home/hadoop/Desktop/test/mergedSample")

    val numSample: DataSet[NumSample] = labledSample.map { s => new NumSample(s.position, 1) }.groupBy(0)
      .reduce { (s1, s2) => new NumSample(s1.position, s1.number + s2.number) }
    //numSample.writeAsText("/home/hadoop/Desktop/test/numSample")

    val uniform: DataSet[Uniform] = numSample.join(mergedSample).where("position").equalTo("position")
      .map { sample => uniformPro(sample) }
    //uniform.map { s => (s.position, s.featureIndex, s.uniform.toList) } writeAsText ("/home/hadoop/Desktop/test/uniform")

    val sum: DataSet[Sum] = uniform.join(updatedSample).where("position", "featureIndex").equalTo("position", "featureIndex") {
      (uni, updatedSample, out: Collector[Sum]) =>
        out.collect(sumPro(uni, updatedSample))
    }
    //sum.map { s => (s.position, s.label, s.featureIndex, s.sum.toList) }.writeAsText("/home/hadoop/Desktop/test/sum")

    val gain: DataSet[Gain] = gainCal(labledSample, sum)
    //gain.map { s => (s.position, s.featureIndex, s.gain.toList) } writeAsText ("/home/hadoop/Desktop/test/gain")

    val splitPlace: DataSet[(String, Int, Double, Double)] = findSplitPlace(gain, uniform)
    //splitPlace.writeAsText("/home/hadoop/Desktop/test/splitPlace")

    val splitedSample: DataSet[LabeledVector] = labledSample.join(splitPlace).where("position").equalTo(0)
      .map { s =>
        if (s._1.feature(s._2._2) < s._2._3)
          new LabeledVector(s._1.position ++ "L", s._1.label, s._1.feature)
        else
          new LabeledVector(s._1.position ++ "R", s._1.label, s._1.feature)
      }
    //splitedSample.map { s => (s.position, s.label, s.feature.toList) }.writeAsText("/home/hadoop/Desktop/test/splitedSample")
    (splitedSample, splitPlace)
  }

  /*
   * unordered
   */
  def inputProCate(nonEmptyDatasets: DataSet[Array[String]]): DataSet[LabeledVectorStr] = {

    val labledSample: DataSet[LabeledVectorStr] = nonEmptyDatasets.map { s =>
      new LabeledVectorStr("", s(0).toDouble, s.drop(14).take(26))
      //new LabeledVectorStr("", s(0).toDouble, s.drop(1).take(2))
    }

    labledSample
  }

  def partitionCate(labledSample: DataSet[LabeledVectorStr]): DataSet[LabeledVectorStr] = {
    val histoSample: DataSet[HistogramStr] = labledSample.flatMap { s =>
      (0 until s.feature.size) map {
        index => new HistogramStr(s.position, s.label, index, s.feature(index), 1)
      }
    }

    //gain
    val gainPre1 = labledSample.map { s => new Frequency(s.position, s.label, 1) }.groupBy("position", "label").reduce {
      (s1, s2) => new Frequency(s1.position, s1.label, s1.frequency + s2.frequency)
    }
    //gainPre1.writeAsText("/home/hadoop/Desktop/test/gainPre1")

    val gainPre2 = gainPre1.groupBy("position").reduce {
      (s1, s2) =>
        new Frequency(s1.position, 0, s1.frequency + s2.frequency)
    }
    //gainPre2.writeAsText("/home/hadoop/Desktop/test/gainPre2")

    val entropy = gainPre1.join(gainPre2).where("position").equalTo("position") {
      (gainPre1, gainPre2, out: Collector[Frequency]) =>
        out.collect(new Frequency(gainPre1.position, gainPre1.label, gainPre1.frequency / gainPre2.frequency))
    }.map {
      s =>
        {
          var re = new Frequency(s.position, s.label, 0)
          if (s.frequency > 0)
            re = new Frequency(s.position, s.label, -s.frequency * log(s.frequency))
          re
        }
    }
    //entropy.writeAsText("/home/hadoop/Desktop/test/entropy")

    val gain = entropy.groupBy("position").reduce {
      (s1, s2) =>
        new Frequency(s1.position, 0, s1.frequency + s2.frequency)
    }
    //gain.writeAsText("/home/hadoop/Desktop/test/gain")

    // gain left
    val gainLeftPre1: DataSet[HistogramStr] = histoSample.groupBy("position", "label", "featureIndex", "featureValue").reduce {
      (s1, s2) => new HistogramStr(s1.position, s1.label, s1.featureIndex, s1.featureValue, s1.frequency + s2.frequency)
    }

    val gainLeftPre2: DataSet[HistogramStr] = gainLeftPre1.groupBy("position", "featureIndex", "featureValue").reduce {
      (s1, s2) => new HistogramStr(s1.position, 0, s1.featureIndex, s1.featureValue, s1.frequency + s2.frequency)
    }

    val entropyLeft = gainLeftPre1.join(gainLeftPre2).where("position", "featureIndex", "featureValue").equalTo("position", "featureIndex", "featureValue") {
      (g1, g2, out: Collector[HistogramStr]) =>
        out.collect(new HistogramStr(g1.position, g1.label, g1.featureIndex, g1.featureValue, g1.frequency / g2.frequency))
    }.map {
      s =>
        {
          var re = new HistogramStr(s.position, s.label, s.featureIndex, s.featureValue, 0)
          if (s.frequency > 0)
            re = new HistogramStr(s.position, s.label, s.featureIndex, s.featureValue, -s.frequency * log(s.frequency))
          re
        }
    }

    val gainLeft = entropyLeft.groupBy("position", "featureIndex", "featureValue").reduce {
      (s1, s2) =>
        new HistogramStr(s1.position, 0, s1.featureIndex, s1.featureValue, s1.frequency + s2.frequency)
    }

    //gainLeft.writeAsText("/home/hadoop/Desktop/test/gainLeft")

    // gain right
    val gainRightPre1 = gainPre1.join(gainLeftPre1).where("position", "label").equalTo("position", "label") {
      (g1, g2, out: Collector[HistogramStr]) =>
        out.collect(new HistogramStr(g2.position, g2.label, g2.featureIndex, g2.featureValue, g1.frequency - g2.frequency))
    }
    val gainRightPre2 = gainRightPre1.groupBy("position", "featureIndex", "featureValue").reduce {
      (s1, s2) => new HistogramStr(s1.position, 0, s1.featureIndex, s1.featureValue, s1.frequency + s2.frequency)
    }

    val entropyRight = gainRightPre1.join(gainRightPre2).where("position", "featureIndex", "featureValue").equalTo("position", "featureIndex", "featureValue") {
      (g1, g2, out: Collector[HistogramStr]) =>
        out.collect(new HistogramStr(g1.position, g1.label, g1.featureIndex, g1.featureValue, g1.frequency / g2.frequency))
    }.map {
      s =>
        {
          var re = new HistogramStr(s.position, s.label, s.featureIndex, s.featureValue, 0)
          if (s.frequency > 0)
            re = new HistogramStr(s.position, s.label, s.featureIndex, s.featureValue, -s.frequency * log(s.frequency))
          re
        }
    }

    val gainRight = entropyRight.groupBy("position", "featureIndex", "featureValue").reduce {
      (s1, s2) =>
        new HistogramStr(s1.position, 0, s1.featureIndex, s1.featureValue, s1.frequency + s2.frequency)
    }

    //gainRight.writeAsText("/home/hadoop/Desktop/test/gainRight")

    //delta
    val deltaPre1 = histoSample.groupBy("position", "featureIndex", "featureValue").reduce {
      (s1, s2) => new HistogramStr(s1.position, 0, s1.featureIndex, s1.featureValue, s1.frequency + s2.frequency)
    }

    val deltaPre2 = labledSample.map { s => 1 }.reduce { _ + _ }

    val delta = deltaPre1.cross(deltaPre2).map {
      s => new HistogramStr(s._1.position, 0, s._1.featureIndex, s._1.featureValue, s._1.frequency / s._2)
    }

    //delta.writeAsText("/home/hadoop/Desktop/test/delta")

    val gainInfo = gain.join(gainLeft).where("position").equalTo("position")
      .map { s => (s._2.position, s._2.featureIndex, s._2.featureValue, s._1.frequency, s._2.frequency) }
      .join(gainRight).where(0, 1, 2).equalTo("position", "featureIndex", "featureValue")
      .map { s => (s._2.position, s._2.featureIndex, s._2.featureValue, s._1._4, s._1._5, s._2.frequency) }
      .join(delta).where(0, 1, 2).equalTo("position", "featureIndex", "featureValue")
      //(1position, 2featureIndex, 3featureValue, 4entropy, 5entropyLeft, 6entropyRight, 7delta )
      .map { s => (s._2.position, s._2.featureIndex, s._2.featureValue, s._1._4, s._1._5, s._1._6, s._2.frequency) }
      .map { s => new GainStr(s._1, s._2, s._3, s._4 - s._7 * s._5 - (1 - s._7) * s._6) }

    //gainInfo.writeAsText("/home/hadoop/Desktop/test/gainInfo")

    val splitPlace = gainInfo.groupBy("position").reduce {
      (s1, s2) =>
        if (s1.gain > s2.gain)
          s1
        else
          s2
    }

    splitPlace.writeAsText("/home/hadoop/Desktop/test/splitCatePlace")

    val splitedSample: DataSet[LabeledVectorStr] = labledSample.join(splitPlace).where("position").equalTo("position")
      .map { s =>
        if (s._1.feature(s._2.featureIndex) == s._2.featureValue)
          new LabeledVectorStr(s._1.position ++ "L", s._1.label, s._1.feature)
        else
          new LabeledVectorStr(s._1.position ++ "R", s._1.label, s._1.feature)
      }

    splitedSample
  }

  /*
     * test error to measure the algorithm accuracy
     */
  def testErrCal(inputTo: DataSet[LabeledVector], totalNum: DataSet[Int]): DataSet[Double] = {
    val real = inputTo.map { s => new Frequency(s.position, s.label, 1) }
      .groupBy("position", "label").reduce { (s1, s2) => new Frequency(s1.position, s1.label, s1.frequency + s2.frequency) }
    val prediction = real.groupBy("position").reduce { (s1, s2) =>
      var re = new Frequency(s1.position, s1.label, 0)
      if (s1.frequency > s2.frequency)
        re = new Frequency(s1.position, s1.label, s1.frequency + s2.frequency)
      else
        re = new Frequency(s1.position, s2.label, s1.frequency + s2.frequency)
      re
    }
    val testErrs = real.join(prediction).where("position").equalTo("position") {
      (real, prediction, out: Collector[Double]) =>
        var count = 0.0
        if (real.label != prediction.label)
          count += real.frequency
        out.collect(count)
    }.reduce { _ + _ }

    val testErr = testErrs.cross(totalNum).map { s => s._1 / s._2 }
    testErr
  }
}