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

object WordCount {

  def main(args: Array[String]) {

    if (!parseParameters(args)) {
      return
    }

    // get execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get data set
    val input = getDataSet(env)
    val sample: DataSet[String] = input.flatMap { _.split("\n") } filter { _.nonEmpty }
    //val sampleTotal: DataSet[Int] = sample.map { s => 1 }.reduce(_ + _)
    val nonEmptysample1: DataSet[List[String]] = sample.map { s => s.split("\t").toList }.filter { !_.contains("") }

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

    val nonEmptysample: DataSet[List[Double]] = nonEmptysample1.map { s =>
      var re = scala.collection.mutable.MutableList[Double]()
      var i = 0

      for (aa <- s) {
        if (i <= 13) {
          re += aa.toDouble
        } else {
          re += toDouble2(aa)
        }
        i += 1
      }
      re.toList
    }

    val labledSample: DataSet[LabeledVector] = nonEmptysample.map { s =>
      new LabeledVector(s(0), s.drop(1).take(13))
    }

    val doneSample: DataSet[Histogram] = labledSample.flatMap {
      s =>
        (0 until s.feature.size) map {
          index => new Histogram(s.label, index, List(Histo(s.feature(index), 1)))
        }
    }

    val updatedSample: DataSet[Histogram] = doneSample.groupBy("label", "featureIndex") reduce {
      (h1, h2) => updatePro(h1, h2)
    }

    val mergedSample: DataSet[MergedHisto] = updatedSample.map { s => new MergedHisto(s.featureIndex, s.histo) }
      .groupBy("featureIndex") reduce {
        (m1, m2) => mergePro(m1, m2)
      }

    val numSample: DataSet[Int] = labledSample.map { s => 1 }.reduce(_ + _)
    //1483

    val uniform: DataSet[Uniform] = numSample.cross(mergedSample).map {
      sample => uniformPro(sample)
    }

    val sum: DataSet[Sum] = uniform.join(updatedSample).where("featureIndex").equalTo("featureIndex") {
      (uni, updatedSample, out: Collector[Sum]) =>
        out.collect(sumPro(uni, updatedSample))
    }

    val numSampleByLabel: DataSet[(Double, Int)] = labledSample.map { s => (s.label, 1) }.groupBy(0).sum(1)
    //(0.0, 1033)
    //(1.0, 450)

    val entropy = sum.groupBy("featureIndex") reduce {
      (h1, h2) => new Sum(0, h1.featureIndex, h1.uniform.zipWithIndex.map { case (e, i) => e + h2.uniform(i) })
    }

    val entropy2 = sum.join(entropy).where("featureIndex").equalTo("featureIndex")
      .map { s =>
        new Sum(s._1.label, s._1.featureIndex, s._1.uniform.zipWithIndex.map {
          case (e, i) =>
            var re = 0.0
            if (s._2.uniform(i) == 0) {
              re = 0.0
            } else {
              re = e / s._2.uniform(i)
            }
            re
        })
      }

    def entroy(q: Double): Double = {
      var re = 0.0
      if (q >= 0) {
        re = -q * log(q)
      }
      re
    }

    val entropy3 = entropy2.groupBy("featureIndex") reduce {
      (h1, h2) => new Sum(0, h1.featureIndex, h1.uniform.zipWithIndex.map { case (e, i) => entropy(e) + entropy(h2.uniform(i)) })
    }

    // emit result
    //test.writeAsText("/home/hadoop/Desktop/test/test")
    updatedSample.writeAsText("/home/hadoop/Desktop/test/updatedSample")
    mergedSample.writeAsText("/home/hadoop/Desktop/test/mergedSample")
    uniform.writeAsText("/home/hadoop/Desktop/test/uniform")
    sum.writeAsText("/home/hadoop/Desktop/test/sum")
    //    entropy.writeAsText("/home/hadoop/Desktop/test/entropy")
    //    entropy2.writeAsText("/home/hadoop/Desktop/test/entropy2")
    //    entropy3.writeAsText("/home/hadoop/Desktop/test/entropy3")
    //entropy2.writeAsText(outputPath)

    // execute program
    env.execute(" Decision Tree ")
  }

  // *************************************************************************
  //  UTIL METHODS
  // *************************************************************************

  private var inputPath: String = null
  private var outputPath: String = null
  private val numFeature = 2 // number of independent features
  private val numBins = 5 // B bins for Update procedure
  private val numSplit = 3 //By default it should be same as numBins
  private val numLevel = 3 // how many levels of tree
  private val leastSample = 5 // least number of samples in one node

  case class Histo(featureValue: Double, frequency: Double)
  case class Histogram(label: Double, featureIndex: Int, histo: List[Histo])
  case class MergedHisto(featureIndex: Int, histo: List[Histo])
  case class Uniform(featureIndex: Int, uniform: List[Double])
  case class Sum(label: Double, featureIndex: Int, uniform: List[Double])
  case class LabeledVector(label: Double, feature: List[Double])

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
   * update process
   */
  def updatePro(h1: Histogram, h2: Histogram): Histogram = {
    var re = new Histogram(0, 0, null)
    var h = (h1.histo ++ h2.histo).toList.sortBy(_.featureValue) //accend

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
        val newFea = h.take(minIndex) ++ List(Histo(newValue, newfrequent)) ++ h.drop(minIndex + 2)
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
    var h = (m1.histo ++ m2.histo).toList.sortBy(_.featureValue) //accend
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
        val newFea = h.take(minIndex) ++ List(Histo(newValue, newfrequent)) ++ h.drop(minIndex + 2)
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
    new Uniform(sample._2.featureIndex, u.toList)
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
    new Sum(label, featureIndex, s.toList)
  }

}
