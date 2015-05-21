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

import org.myorg.quickstart.Vector

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
      (h1, h2) => //new Histogram(h1.label, h1.featureIndex, List(Histo(h1.histo(0).featureValue, h1.histo(0).frequency+h2.histo(0).frequency)))
        {
          var re = new Histogram(0, 0, null)
          var h = h1.histo ++ h2.histo
          if (h.size <= numBins) {
            re = new Histogram(h1.label, h1.featureIndex, h)
          } else {
            while (h.size > numBins) {
              var minIndex = 0
              var minValue = Integer.MAX_VALUE.toDouble
              for (i <- 0 to h.size - 2) {
                if (h(i).featureValue - h(i + 1).featureValue < minValue) {
                  minIndex = i
                  minValue = h(i).featureValue - h(i + 1).featureValue
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
    }

    val mergedSample: DataSet[MergedHisto] = updatedSample.map { s => new MergedHisto(s.featureIndex, s.histo) }
      .groupBy("featureIndex") reduce {
        (h1, h2) =>
          {
            var re = new MergedHisto(0, null)
            var h = h1.histo ++ h2.histo
            if (h.size <= numBins) {
              re = new MergedHisto(h1.featureIndex, h)
            } else {
              while (h.size > numBins) {
                var minIndex = 0
                var minValue = Integer.MAX_VALUE.toDouble
                for (i <- 0 to h.size - 2) {
                  if (h(i).featureValue - h(i + 1).featureValue < minValue) {
                    minIndex = i
                    minValue = h(i).featureValue - h(i + 1).featureValue
                  }
                }
                val newfrequent = h(minIndex).frequency + h(minIndex + 1).frequency
                val newValue = (h(minIndex).featureValue * h(minIndex).frequency + h(minIndex + 1).featureValue * h(minIndex + 1).frequency) / newfrequent
                val newFea = h.take(minIndex) ++ List(Histo(newValue, newfrequent)) ++ h.drop(minIndex + 2)
                h = newFea
              }
              re = new MergedHisto(h1.featureIndex, h)
            }
            //re.histo.toList.sortBy(_.featureValue) 
            new MergedHisto(re.featureIndex, re.histo.toList.sortBy(_.featureValue)) //ascend
          }
      }
    val numSample: DataSet[Int] = labledSample.map { s => 1 }.reduce(_ + _)
    //1483

    val uniform: DataSet[Uniform] = numSample.cross(mergedSample).map {
      sample =>
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

    val numSampleByLabel: DataSet[(Double, Int)] = labledSample.map { s => (s.label, 1) }.groupBy(0).sum(1)
    //(0.0, 1033)
    //(1.0, 450)

    // emit result
    //sample.writeAsCsv(outputPath, "\n", "|")
    uniform.writeAsText(outputPath)

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

}
