package org.myorg.quickstart

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.common.operators.Order

import org.apache.flink.api.common.functions.GroupReduceFunction
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

    // get customer data set: (custkey, name, address, nationkey, acctbal) 
    val input = getDataSet(env)

    val numFeature = 2 // number of independent features
    val numBins = 5 // B bins for Update procedure
    val numSplit = 3 //By default it should be same as numBins
    val numLevel = 3 // how many levels of tree
    val leastSample = 5 // least number of samples in one node
    println("-- Welcom to Decision Tree --")

    //val output = input.groupBy(1).aggregate(Aggregations.SUM, 0).and(Aggregations.MIN, 2)

    val samples = input.map { s => new Sample(s._1, s._2) }

    //val test = samples.sortPartition("feature",Order.ASCENDING).setParallelism(1)

    val totalSample = input.map { s => 1 }.reduce(_ + _).map { s => (0.0, s) }

    val numSample = input.map { s => (s._1, 1) }.groupBy(0).sum(1) //grouped by label

    val numLabel = numSample.map { s => 1 }.reduce(_ + _)

    val adjacencySamples = samples
      .groupBy("label").reduceGroup(new GroupReduceFunction[Sample, AdjacencySample] {
        override def reduce(values: Iterable[Sample], out: Collector[AdjacencySample]): Unit = {
          var outputId = 0.0
          val outputList = values.asScala map { t => outputId = t.label; new Histo(t.feature, 1) }
          out.collect(new AdjacencySample(outputId, outputList))
        }
      })

    val updatedSample = numSample.join(adjacencySamples).where(0).equalTo("label") {
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

    val test = numSample.join(updatedSample).where(0).equalTo("label") {
      (num, sample, out: Collector[Double]) =>
        val features = sample.features.toList.sortBy(_.value) //ascend
        val len = features.length

        val b = 15 // parameter
        var i = 0
        var s = 0.0
        var result = 0.0

        if (len == 0) {
          result = 0.0
        } else if (b >= features(len - 1).value) {
          result = num._2
        } else if (b < features(0).value) {
          result = 0.0
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
          s = (mi + mb) * (b - pi) / (2 * (pii - pi))

          for (j <- 0 to i - 1) {
            s += features(j).frequent
          }
          s += features(i).frequent / 2

        }
        out.collect(s)
    }

    val uniform = mergedSample.join(totalSample).where(0).equalTo(0) {
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

    // emit result
    //test.writeAsCsv(outputPath, "\n", "|")
    uniform.writeAsText(outputPath)

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

//0.0|List(Histo(45.0,1.0), Histo(32.666666666666664,3.0), Histo(19.333333333333332,3.0), Histo(9.5,2.0), Histo(2.0,1.0))//
//1.0|List(Histo(84.5,4.0), Histo(72.0,1.0), Histo(64.0,1.0), Histo(49.5,2.0), Histo(29.0,2.0))

//2|List(Histo(45.0,1.0), Histo(32.666666666666664,3.0), Histo(19.333333333333332,3.0), Histo(9.5,2.0), Histo(2.0,1.0), Histo(84.5,4.0), Histo(72.0,1.0), Histo(64.0,1.0), Histo(49.5,2.0), Histo(29.0,2.0))
//0.0|List(Histo(84.5,4.0), Histo(68.0,2.0), Histo(48.0,3.0), Histo(26.75,8.0), Histo(7.0,3.0))

//3.2750646365986786
//0.0
