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
    nonEmptyDatasets.map { s => s.toList }.writeAsText("/home/hadoop/Desktop/test/nonEmptyDatasets")

    val labledSample: DataSet[LabeledVector] = inputPro(nonEmptyDatasets)
    labledSample.map { s => (s.position, s.label, s.feature.toList) }.writeAsText("/home/hadoop/Desktop/test/labledSample")

    val histoSample: DataSet[Histogram] = labledSample.flatMap { s =>
      (0 until s.feature.size) map {
        index => new Histogram(s.position, s.label, index, Array(Histo(s.feature(index), 1)))
      }
    }
    histoSample.map { s => (s.position, s.label, s.featureIndex, s.histo.toList) }.writeAsText("/home/hadoop/Desktop/test/histoSample")

    val updatedSample: DataSet[Histogram] = histoSample.groupBy("position", "label", "featureIndex") reduce {
      (h1, h2) => updatePro(h1, h2)
    }
    updatedSample.map { s => (s.position, s.label, s.featureIndex, s.histo.toList) }.writeAsText("/home/hadoop/Desktop/test/updatedSample")

    val mergedSample: DataSet[MergedHisto] = updatedSample.map { s => new MergedHisto(s.position, s.featureIndex, s.histo) }
      .groupBy("position", "featureIndex") reduce {
        (m1, m2) => mergePro(m1, m2)
      }
    mergedSample.map { s => (s.position, s.featureIndex, s.histo.toList) }.writeAsText("/home/hadoop/Desktop/test/mergedSample")

    val numSample: DataSet[NumSample] = labledSample.map { s => new NumSample(s.position, 1) }
      .groupBy("position").reduce { (s1, s2) => new NumSample(s1.position, s1.number + s2.number) }
    numSample.map { s => (s.position, s.number) }.writeAsText("/home/hadoop/Desktop/test/numSample")

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

  case class LabeledVector(position: ArrayBuffer[Char], label: Double, feature: Array[Double])
  case class Histo(featureValue: Double, frequency: Double)
  case class Histogram(position: ArrayBuffer[Char], label: Double, featureIndex: Int, histo: Array[Histo])
  case class MergedHisto(position: ArrayBuffer[Char], featureIndex: Int, histo: Array[Histo])
  case class NumSample(position: ArrayBuffer[Char], number: Int)

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

}