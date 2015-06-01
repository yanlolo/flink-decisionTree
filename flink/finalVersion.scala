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
      var re = new Array[Double](40)
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

}