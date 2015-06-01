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

    val labledSample = inputPro(nonEmptyDatasets)
    //labledSample.map { s => (s.position, s.label, s.feature.toList) }.writeAsText("/home/hadoop/Desktop/test/labledSample")

    val histoSample: DataSet[Histogram] = labledSample.flatMap { s =>
      (0 until s.feature.size) map {
        index => new Histogram(s.label, index, Array(Histo(s.feature(index), 1)))
      }
    }
    //histoSample.map { s => (s.label, s.featureIndex, s.histo.toList) }.writeAsText("/home/hadoop/Desktop/test/histoSample")

    val updatedSample: DataSet[Histogram] = histoSample.groupBy("label", "featureIndex") reduce {
      (h1, h2) => updatePro(h1, h2)
    }
    //updatedSample.map { s => (s.label, s.featureIndex, s.histo.toList) }.writeAsText("/home/hadoop/Desktop/test/updatedSample")

    val mergedSample: DataSet[MergedHisto] = updatedSample.map { s => new MergedHisto(s.featureIndex, s.histo) }
      .groupBy("featureIndex") reduce {
        (m1, m2) => mergePro(m1, m2)
      }
    //mergedSample.map { s => (s.featureIndex, s.histo.toList) }.writeAsText("/home/hadoop/Desktop/test/mergedSample")

    val numSample: DataSet[Int] = labledSample.map { s => 1 }.reduce(_ + _)
    //1483

    val uniform: DataSet[Uniform] = numSample.cross(mergedSample).map {
      sample => uniformPro(sample)
    }
    //uniform.map { s => (s.featureIndex, s.uniform.toList) }.writeAsText("/home/hadoop/Desktop/test/uniform")

    val sum: DataSet[Sum] = uniform.join(updatedSample).where("featureIndex").equalTo("featureIndex") {
      (uni, updatedSample, out: Collector[Sum]) =>
        out.collect(sumPro(uni, updatedSample))
    }
    //sum.map { s => (s.label, s.featureIndex, s.sum.toList) }.writeAsText("/home/hadoop/Desktop/test/sum")

    val gain: DataSet[(Int, Array[Double])] = gainCal(labledSample, sum)
    //gain.map { s => (s._1, s._2.toList) }.writeAsText("/home/hadoop/Desktop/test/gain")

    val splitPlace: DataSet[(Int, Double)] = findSplitPlace(gain, uniform)
    //splitPlace.writeAsText("/home/hadoop/Desktop/test/splitPlace")

    val splitedSample = labledSample.cross(splitPlace)
      .map { s =>
        if (s._1.feature(s._2._1) < s._2._2)
          new LabeledVector(s._1.position += ('L'), s._1.label, s._1.feature)
        else
          new LabeledVector(s._1.position += ('R'), s._1.label, s._1.feature)
      }
    splitedSample.map { s => (s.position.toList, s.label, s.feature.toList) }.writeAsText("/home/hadoop/Desktop/test/splitedSample")

    // execute program
    env.execute(" Decision Tree ")
  }

  private var inputPath: String = null
  private var outputPath: String = null
  private val numFeature = 2 // number of independent features
  private val numBins = 5 // B bins for Update procedure
  private val numSplit = 3 //By default it should be same as numBins
  private val numLevel = 3 // how many levels of tree
  private val leastSample = 5 // least number of samples in one node

  case class LabeledVector(position: ArrayBuffer[Char], label: Double, feature: Array[Double])
  case class Histo(featureValue: Double, frequency: Double)
  case class Histogram(label: Double, featureIndex: Int, histo: Array[Histo])
  case class MergedHisto(featureIndex: Int, histo: Array[Histo])
  case class Uniform(featureIndex: Int, uniform: Array[Double])
  case class Sum(label: Double, featureIndex: Int, sum: Array[Double])

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
  def toDouble1(c: Char): Double = {
    var re = 0.0
    if (c == '0')
      re = 0
    if (c == '1')
      re = 1
    if (c == '2')
      re = 2
    if (c == '3')
      re = 3
    if (c == '4')
      re = 4
    if (c == '5')
      re = 5
    if (c == '6')
      re = 6
    if (c == '7')
      re = 7
    if (c == '8')
      re = 8
    if (c == '9')
      re = 9
    if (c == 'a')
      re = 10
    if (c == 'b')
      re = 11
    if (c == 'c')
      re = 12
    if (c == 'd')
      re = 13
    if (c == 'e')
      re = 14
    if (c == 'f')
      re = 15

    re
  }

  //Hex
  //    def toDouble2(s: String): Double = { 
  //      var re = 0.0
  //      for (ss <- s) {
  //        re = re * 16 + toDouble1(ss)
  //      }
  //      re
  //    }

  def toDouble2(s: String): Double = {
    var re = 0.0
    for (ss <- s) {
      re += toDouble1(ss)
    }
    re
  }

  def inputPro(nonEmptyDatasets: DataSet[Array[String]]): DataSet[LabeledVector] = {
    val nonEmptySample: DataSet[Array[Double]] = nonEmptyDatasets.map { s =>
      var re = new Array[Double](3)
      //var re = new Array[Double](40)
      var i = 0

      for (aa <- s) {
        if (i <= 13)
          re(i) = aa.toDouble
        else
          re(i) = toDouble2(aa)
        i += 1
      }

      re
    }

    val labledSample: DataSet[LabeledVector] = nonEmptySample.map { s =>
      new LabeledVector(ArrayBuffer('C'), s(0), s.drop(1).take(13))
    }

    labledSample
  }

  /*
   * update process
   */
  def updatePro(h1: Histogram, h2: Histogram): Histogram = {
    var re = new Histogram(0, 0, null)
    var h = (h1.histo ++ h2.histo).sortBy(_.featureValue) //accend

    if (h.size <= numBins) {
      re = new Histogram(h1.label, h1.featureIndex, h)
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
      re = new Histogram(h1.label, h1.featureIndex, h)
    }
    re
  }

  /*
   * merge process
   */
  def mergePro(m1: MergedHisto, m2: MergedHisto): MergedHisto = {
    var re = new MergedHisto(0, null)
    var h = (m1.histo ++ m2.histo).sortBy(_.featureValue) //accend
    if (h.size <= numBins) {
      re = new MergedHisto(m1.featureIndex, h)
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
      re = new MergedHisto(m1.featureIndex, h)
    }
    re
  }

  /*
   * uniform process
   */
  def uniformPro(sample: (Int, MergedHisto)): Uniform = {
    val histo = sample._2.histo //ascend
    val len = histo.length
    var u = new Array[Double](numSplit - 1)

    for (j <- 1 to numSplit - 1) {
      var s = j * sample._1 / numSplit.toDouble

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
    new Uniform(sample._2.featureIndex, u)
  }

  /*
   * sum process
   */
  def sumPro(uni: Uniform, updatedSample: Histogram): Sum = {
    val label = updatedSample.label
    val featureIndex = updatedSample.featureIndex
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
    new Sum(label, featureIndex, s)
  }

  /*
   * calculate the entropy
   */
  def entropyLeftCal(sum: DataSet[Sum]): DataSet[Sum] = {

    val entropy: DataSet[Sum] = sum.groupBy("featureIndex") reduce {
      (h1, h2) => new Sum(0, h1.featureIndex, h1.sum.zipWithIndex.map { case (e, i) => e + h2.sum(i) })
    }

    val entropy2: DataSet[Sum] = sum.join(entropy).where("featureIndex").equalTo("featureIndex")
      .map { s =>
        new Sum(s._1.label, s._1.featureIndex, s._1.sum.zipWithIndex.map {
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

    val entropy3: DataSet[Sum] = entropy2.groupBy("featureIndex") reduce {
      (h1, h2) =>
        new Sum(0, h1.featureIndex, h1.sum.zipWithIndex.map {
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

  def entropyCal(numSampleByLabel: DataSet[(Double, Double)]): DataSet[(Double, Double)] = {
    val entropy: DataSet[(Double, Double)] = numSampleByLabel.reduce {
      (h1, h2) => (0, h1._2 + h2._2)
    }

    val entropy2: DataSet[(Double, Double)] = numSampleByLabel.cross(entropy).map {
      s => (s._1._1, s._1._2 / s._2._2)
    }

    val entropy3: DataSet[(Double, Double)] = entropy2.reduce {
      (h1, h2) =>
        {
          var a = 0.0
          if (h1._2 > 0) {
            a = -h1._2 * log(h1._2)
          }

          var b = 0.0
          if (h2._2 > 0) {
            b = -h2._2 * log(h2._2)
          }
          (0, a + b)
        }
    }
    entropy3
  }

  /*
   *  calculate gain for every split candidates
   */
  def gainCal(labledSample: DataSet[LabeledVector], sum: DataSet[Sum]): DataSet[(Int, Array[Double])] = {

    val numSampleByLabel: DataSet[(Double, Double)] = labledSample.map { s => (s.label, 1) }.groupBy(0).sum(1)
      .map(s => (s._1, s._2.toDouble))
    //(0.0, 1033)
    //(1.0, 450)

    val entropy = entropyCal(numSampleByLabel)
    val entropyLeft = entropyLeftCal(sum)
    val sumRight = sum.map { s => (s.label, s.featureIndex.toDouble, s.sum) }
      .join(numSampleByLabel).where(1).equalTo(0) {
        (sum, numSampleByLabel, out: Collector[Sum]) =>
          out.collect(new Sum(sum._1, sum._2.toInt, sum._3.zipWithIndex.map { case (e, i) => numSampleByLabel._2 - e }))
      }
    val entropyRight = entropyLeftCal(sumRight)

    val delta1: DataSet[Sum] = sum.groupBy("featureIndex") reduce {
      (h1, h2) => new Sum(0, h1.featureIndex, h1.sum.zipWithIndex.map { case (e, i) => e + h2.sum(i) })
    }

    val totalSample = labledSample.map { s => 1 }.reduce(_ + _)

    val delta = delta1.cross(totalSample).map {
      s => (s._1.featureIndex, s._1.sum.zipWithIndex.map { case (e, i) => e / s._2 })
    }

    val gain = entropyLeft.join(entropyRight).where("label", "featureIndex").equalTo("label", "featureIndex").map {
      s => (s._1.featureIndex, s._1.sum, s._2.sum)
    }.join(delta).where(0).equalTo(0).cross(entropy).map {
      s =>
        (s._1._1._1, s._1._1._2.zipWithIndex.map {
          case (e, i) => s._2._2 - s._1._2._2(i) * e - (1 - s._1._2._2(i)) * s._1._1._3(i)
        })
    }
    gain
  }

  /*
   * find the split place
   */
  def findSplitPlace(gain: DataSet[(Int, Array[Double])], uniform: DataSet[Uniform]): DataSet[(Int, Double)] = {
    // (the split feature, the uniform place)
    val splitPlace1 = gain.map {
      s =>
        val feature = s._2
        var max = Integer.MIN_VALUE.toDouble
        var maxIndex = 0
        for (i <- 0 until feature.length) {
          if (feature(i) > max) {
            max = feature(i)
            maxIndex = i
          }
        }
        (s._1, maxIndex)
    }.reduce { (s1, s2) =>
      var re = (0, 0)
      if (s1._2 <= s2._2)
        re = s2
      else
        re = s1
      re
    }

    val splitPlace = splitPlace1.cross(uniform).filter { s => (s._2.featureIndex == s._1._1) } // get the matched feature
      .map {
        s => (s._1._1, s._2.uniform(s._1._2))
      }
    splitPlace
  }

}