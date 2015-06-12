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

    val labledSample: DataSet[LabeledVectorStr] = inputProCat(nonEmptyDatasets)
    val splitedSample: DataSet[LabeledVectorStr] = partitionCate(labledSample)

    splitedSample.map { s => (s.position, s.label, s.feature.toList) } writeAsText ("/home/hadoop/Desktop/test/splitedSample")

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

  def inputProCat(nonEmptyDatasets: DataSet[Array[String]]): DataSet[LabeledVectorStr] = {

    val labledSample: DataSet[LabeledVectorStr] = nonEmptyDatasets.map { s =>
      //new LabeledVectorStr("", s(0).toDouble, s.drop(14).take(26))
      new LabeledVectorStr("", s(0).toDouble, s.drop(1).take(2))
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
    gainPre1.writeAsText("/home/hadoop/Desktop/test/gainPre1")

    val gainPre2 = gainPre1.groupBy("position").reduce {
      (s1, s2) =>
        new Frequency(s1.position, 0, s1.frequency + s2.frequency)
    }
    gainPre2.writeAsText("/home/hadoop/Desktop/test/gainPre2")

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
    entropy.writeAsText("/home/hadoop/Desktop/test/entropy")

    val gain = entropy.groupBy("position").reduce {
      (s1, s2) =>
        new Frequency(s1.position, 0, s1.frequency + s2.frequency)
    }
    gain.writeAsText("/home/hadoop/Desktop/test/gain")

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

    gainLeft.writeAsText("/home/hadoop/Desktop/test/gainLeft")

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

    gainRight.writeAsText("/home/hadoop/Desktop/test/gainRight")

    //delta
    val deltaPre1 = histoSample.groupBy("position", "featureIndex", "featureValue").reduce {
      (s1, s2) => new HistogramStr(s1.position, 0, s1.featureIndex, s1.featureValue, s1.frequency + s2.frequency)
    }

    val deltaPre2 = labledSample.map { s => 1 }.reduce { _ + _ }

    val delta = deltaPre1.cross(deltaPre2).map {
      s => new HistogramStr(s._1.position, 0, s._1.featureIndex, s._1.featureValue, s._1.frequency / s._2)
    }

    delta.writeAsText("/home/hadoop/Desktop/test/delta")

    val gainInfo = gain.join(gainLeft).where("position").equalTo("position")
      .map { s => (s._2.position, s._2.featureIndex, s._2.featureValue, s._1.frequency, s._2.frequency) }
      .join(gainRight).where(0, 1, 2).equalTo("position", "featureIndex", "featureValue")
      .map { s => (s._2.position, s._2.featureIndex, s._2.featureValue, s._1._4, s._1._5, s._2.frequency) }
      .join(delta).where(0, 1, 2).equalTo("position", "featureIndex", "featureValue")
      //(1position, 2featureIndex, 3featureValue, 4entropy, 5entropyLeft, 6entropyRight, 7delta )
      .map { s => (s._2.position, s._2.featureIndex, s._2.featureValue, s._1._4, s._1._5, s._1._6, s._2.frequency) }
      .map { s => new GainStr(s._1, s._2, s._3, s._4 - s._7 * s._5 - (1 - s._7) * s._6) }

    gainInfo.writeAsText("/home/hadoop/Desktop/test/gainInfo")

    val splitPlace = gainInfo.groupBy("position").reduce {
      (s1, s2) =>
        if (s1.gain > s2.gain)
          s1
        else
          s2
    }

    splitPlace.writeAsText("/home/hadoop/Desktop/test/splitPlace")

    val splitedSample: DataSet[LabeledVectorStr] = labledSample.join(splitPlace).where("position").equalTo("position")
      .map { s =>
        if (s._1.feature(s._2.featureIndex) == s._2.featureValue)
          new LabeledVectorStr(s._1.position ++ "L", s._1.label, s._1.feature)
        else
          new LabeledVectorStr(s._1.position ++ "R", s._1.label, s._1.feature)
      }

    splitedSample
  }

}