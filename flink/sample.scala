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

    val numFeature = 2 // number of independent features
    val numBins = 5 // B bins for Update procedure
    val numSplit = 3 //By default it should be same as numBins
    val numLevel = 3 // how many levels of tree
    val leastSample = 5 // least number of samples in one node

    println("-- Welcom to Decision Tree --")

    val samples = input.map { s => new Sample(s._1, s._2) }

    val numSampleByLabel = samples.map { s => (s.label, 1) }.groupBy(0).sum(1) //grouped by label
    //(0.0, 10)
    //(1.0, 10)
    val numLabel = numSampleByLabel.map { s => 1 }.reduce(_ + _)
    //2
    val numSample = numSampleByLabel.map { s => s._2 }.reduce(_ + _)
    //20
    val totalSample = numSample.cross(numSampleByLabel)
    //(20,(0.0,10))
    //(20,(1.0,10))

    val adjacencySamples = samples
      .groupBy("label").reduceGroup(new GroupReduceFunction[Sample, AdjacencySample] {
        override def reduce(values: Iterable[Sample], out: Collector[AdjacencySample]): Unit = {
          var outputId = 0.0
          val outputList = values.asScala map { t => outputId = t.label; new Histo(t.feature, 1) }
          out.collect(new AdjacencySample(outputId, outputList))
        }
      })
    //AdjacencySample(0.0,List(Histo(23.0,1.0), Histo(19.0,1.0), Histo(10.0,1.0), Histo(16.0,1.0), Histo(36.0,1.0), Histo(2.0,1.0), Histo(9.0,1.0), Histo(32.0,1.0), Histo(30.0,1.0), Histo(45.0,1.0)))
    //AdjacencySample(1.0,List(Histo(46.0,1.0), Histo(78.0,1.0), Histo(83.0,1.0), Histo(30.0,1.0), Histo(64.0,1.0), Histo(28.0,1.0), Histo(87.0,1.0), Histo(90.0,1.0), Histo(53.0,1.0), Histo(72.0,1.0)))

    val updatedSample = numSampleByLabel.join(adjacencySamples).where(0).equalTo("label") {
      (num, adjacenct, out: Collector[AdjacencySample]) =>
        val label = adjacenct.label
        var features = adjacenct.features.toList.sortBy(-_.value) //descending

        for (j <- 0 to num._2 - numBins - 1) {
          var minIndex = 0
          var minValue = Integer.MAX_VALUE.toDouble
          for (i <- 0 to features.size - 2) {
            if (features(i).value - features(i + 1).value < minValue) {
              minIndex = i
              minValue = features(i).value - features(i + 1).value
            }
          }
          val newfrequent = features(minIndex).frequent + features(minIndex + 1).frequent
          val newValue = (features(minIndex).value * features(minIndex).frequent + features(minIndex + 1).value * features(minIndex + 1).frequent) / newfrequent
          val newFea = features.take(minIndex) ++ List(Histo(newValue, newfrequent)) ++ features.drop(minIndex + 2)
          features = newFea
        }
        out.collect(new AdjacencySample(label, features))
    }
    //AdjacencySample(0.0,List(Histo(45.0,1.0), Histo(32.666666666666664,3.0), Histo(19.333333333333332,3.0), Histo(9.5,2.0), Histo(2.0,1.0)))
    //AdjacencySample(1.0,List(Histo(84.5,4.0), Histo(72.0,1.0), Histo(64.0,1.0), Histo(49.5,2.0), Histo(29.0,2.0)))

    val preMergedSample = updatedSample.map { s => (1, s.features) }
      .reduce((s1, s2) => (s1._1 + s2._1, s1._2 ++ s2._2))

    val mergedSample = preMergedSample.join(preMergedSample).where(0).equalTo(0) {
      (merged, tt, out: Collector[AdjacencySample]) =>
        val label = 0.0
        var features = merged._2.toList.sortBy(-_.value)
        val len = features.length

        for (j <- 0 to len - numBins - 1) {
          var minIndex = 0
          var minValue = Integer.MAX_VALUE.toDouble
          for (i <- 0 to features.size - 2) {
            if (features(i).value - features(i + 1).value < minValue) {
              minIndex = i
              minValue = features(i).value - features(i + 1).value
            }
          }
          val newfrequent = features(minIndex).frequent + features(minIndex + 1).frequent
          val newValue = (features(minIndex).value * features(minIndex).frequent + features(minIndex + 1).value * features(minIndex + 1).frequent) / newfrequent
          val newFea = features.take(minIndex) ++ List(Histo(newValue, newfrequent)) ++ features.drop(minIndex + 2)
          features = newFea
        }
        out.collect(new AdjacencySample(label, features))
    }
    //AdjacencySample(0.0,List(Histo(84.5,4.0), Histo(68.0,2.0), Histo(48.0,3.0), Histo(26.75,8.0), Histo(7.0,3.0)))

    val tt = numSample.map { s => (0.0, s) }
    val uniform = mergedSample.join(tt).where(0).equalTo(0) {
      (sample, tt, out: Collector[List[Double]]) =>
        val features = sample.features.toList.sortBy(_.value) //ascend

        val len = features.length
        var u = new Array[Double](numSplit - 1)

        for (j <- 1 to numSplit - 1) {
          var s = j * tt._2 / numSplit.toDouble

          if (s <= features(0).frequent) {
            u(j - 1) = features(0).value
          } else {

            var i = 0
            var sumP = 0.0
            // Sum Procedure
            while (sumP < s) {
              if (i == 0)
                sumP += features(i).frequent / 2
              else
                sumP += features(i).frequent / 2 + features(i - 1).frequent / 2
              i += 1
            }
            i -= 2

            var d = s - (sumP - features(i + 1).frequent / 2 - features(i).frequent / 2)
            var a = features(i + 1).frequent - features(i).frequent
            var b = 2 * features(i).frequent
            var c = -2 * d
            var z = if (a == 0) -c / b else (-b + sqrt(pow(b, 2) - 4 * a * c)) / (2 * a)

            u(j - 1) = features(i).value + (features(i + 1).value - features(i).value) * z
          }
        }
        out.collect(u.toList)
    }
    //List(25.91607980660953, 53.839743969093604)

    val preSum = numSampleByLabel.cross(uniform).map { s => new Test(s._1._1, s._1._2, s._2) }
    //((0.0, 10, List(25.91607980660953, 53.839743969093604))
    //((1.0, 10, List(25.91607980660953, 53.839743969093604))

    val sum = preSum.join(updatedSample).where("label").equalTo("label") {
      (num, sample, out: Collector[(Double, List[Double])]) =>
        val label = sample.label
        val features = sample.features.toList.sortBy(_.value) //ascend
        val len = features.length
        var s = new Array[Double](num.uniform.length)

        var k = 0
        for (b <- num.uniform) {
          var i = 0

          if (len == 0) {
            s(k) = 0.0
          } else if (b >= features(len - 1).value) {
            s(k) = num.frequent
          } else if (b < features(0).value) {
            s(k) = 0.0
          } else {
            while (b >= features(i).value) {
              i += 1
            }
            i -= 1

            val mi = features(i).frequent
            val mii = features(i + 1).frequent
            val pi = features(i).value
            val pii = features(i + 1).value
            val mb = mi + (mii - mi) * (b - pi) / (pii - pi)
            s(k) = (mi + mb) * (b - pi) / (2 * (pii - pi))

            for (j <- 0 to i - 1) {
              s(k) += features(j).frequent
            }
            s(k) += features(i).frequent / 2

          }
          k += 1
        }

        out.collect((label, s.toList))
    }
    //(0.0, List(5.981117956487145, 10.0))
    //(1.0, List(0.0, 3.553797318644815))

    val entropyLeft0 = sum.map(s => (s._1, s._2(0), s._2(1)))
    val entropyLeft1 = entropyLeft0.map(s => (s._2, s._3)).reduce((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2))
    val entropyLeft2 = entropyLeft0.cross(entropyLeft1).map { s => (s._1._1, s._1._2 / s._2._1, s._1._3 / s._2._2) }
    //(0.0,1.0,0.7378006152005714)
    //(1.0,0.0,0.26219938479942856)

    val entropyLeft3 = entropyLeft2.map { s =>
      var x = 0.0
      var y = 0.0
      if (s._2 == 0)
        x = 0
      else x = -s._2 * log(s._2)
      if (s._3 == 0)
        y = 0
      else y = -s._3 * log(s._3)
      (s._1, x, y)
    }
    //(0.0, -0.0, 0.22435163581096879)
    //(1.0, 0.0, 0.3509932206095104)

    val gainLeft = entropyLeft3.map(s => (s._2, s._3)).reduce((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2))
    //(0.0,0.5753448564204792)

    val entropyRight0 = entropyLeft0.join(numSampleByLabel).where(0).equalTo(0)
      .map { s => (s._1._1, s._2._2 - s._1._2, s._2._2 - s._1._3) }
    //(0.0, 4.018882043512855, 0.0)
    //(1.0, 10.0, 6.446202681355185)
    val entropyRight1 = entropyRight0.map(s => (s._2, s._3)).reduce((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2))
    val entropyRight2 = entropyRight0.cross(entropyRight1).map { s => (s._1._1, s._1._2 / s._2._1, s._1._3 / s._2._2) }
    val entropyRight3 = entropyRight2.map { s =>
      var x = 0.0
      var y = 0.0
      if (s._2 == 0)
        x = 0
      else x = -s._2 * log(s._2)
      if (s._3 == 0)
        y = 0
      else y = -s._3 * log(s._3)
      (s._1, x, y)
    }
    val gainRight = entropyRight3.map(s => (s._2, s._3)).reduce((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2))
    //(0.5991488600923703, 0.0)

    val tau = entropyLeft1.cross(numSample).map { s => (s._1._1 / s._2, s._1._2 / s._2) }
    //(0.29905589782435726, 0.6776898659322408)

    val entropy = totalSample.map { s => s._2._2.toDouble / s._1 }
      .reduce((s1, s2) => (-s1 * log(s1) - s2 * log(s2)))
    // 0.6931471805599453

    val gain = entropy.cross(tau).cross(gainLeft).cross(gainRight)
      //(((0.6931471805599453,(0.29905589782435726,0.6776898659322408)),(0.0,0.5753448564204792)),(0.5991488600923703,0.0))
      .map { s => (s._1._1._1 - s._1._1._2._1 * s._1._2._1 - s._1._1._2._2 * s._2._1, s._1._1._1 - s._1._1._2._1 * s._1._2._2 - s._1._1._2._2 * s._2._2) }
    //(0.2871100698904919,0.521086907964493)

    val split = gain.cross(uniform).map { s => List((s._1._1, s._2(0)), (s._1._2, s._2(1))) }
      .map { s => s.sortBy(-_._1) }.map(s => s(0)._2)
    //53.839743969093604

    // emit result
    //test.writeAsCsv(outputPath, "\n", "|")
    split.writeAsText(outputPath)

    // execute program
    env.execute(" Decision Tree ")
  }

  // *************************************************************************
  //  UTIL METHODS
  // *************************************************************************

  private var inputPath: String = null
  private var outputPath: String = null

  case class Sample(label: Double, feature: Double)
  case class Histo(value: Double, frequent: Double)
  case class AdjacencySample(label: Double, features: scala.collection.Iterable[Histo])
  case class Test(label: Double, frequent: Double, uniform: List[Double])

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

  private def getDataSet(env: ExecutionEnvironment): DataSet[(Double, Double)] = {
    println(" start input")
    env.readCsvFile[(Double, Double)](inputPath, fieldDelimiter = " ", lineDelimiter = "\n",
      includedFields = Array(0, 1))
  }

}

