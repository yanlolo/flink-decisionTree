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
    val nonEmptyDatasets = datasets.map { s => s.split("\t") }.filter { !_.contains("") }
      .map { s =>
        var re = (" ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ")
        if (s(0) == "0")
          re = ("No", s(14), s(15), s(16), s(17), s(18), s(19), s(20), s(21), s(22), s(23), s(24), s(25), s(26), s(27), s(28), s(29), s(30), s(31), s(32), s(33), s(34))
        else if (s(0) == "1")
          re = ("Yes", s(14), s(15), s(16), s(17), s(18), s(19), s(20), s(21), s(22), s(23), s(24), s(25), s(26), s(27), s(28), s(29), s(30), s(31), s(32), s(33), s(34))
        re
      }
    //nonEmptyDatasets.writeAsText("/home/hadoop/Desktop/test/nonEmptyDatasets")
    //.map { s => s.toList }
    nonEmptyDatasets.writeAsCsv("/home/hadoop/Desktop/test/nonEmptyDatasets", "\n", " ")

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
  private val numLevel = 1 // how many levels of tree
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

}
