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
      new LabeledVector(s(0), s.drop(1))
    }

    // emit result
    //sample.writeAsCsv(outputPath, "\n", "|")
    labledSample.writeAsText(outputPath)

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

  case class LabeledVector(label: Double, feature: List[Double]) extends Vector
  //
  //    override def equals(obj: Any): Boolean = {
  //      obj match {
  //        case labeledVector: LabeledVector =>
  //          vector.equals(labeledVector.vector) && label.equals(labeledVector.label)
  //        case _ => false
  //      }
  //    }
  //
  //    override def toString: String = {
  //      s"LabeledVector($label, $vector)"
  //    }

  //
  //  case class Sample(label: Double, feature: Double)
  //  case class Histo(value: Double, frequent: Double)
  //  case class AdjacencySample(label: Double, features: scala.collection.Iterable[Histo])
  //  case class Test(label: Double, frequent: Double, uniform: List[Double])

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
