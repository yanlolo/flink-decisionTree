package org.myorg.quickstart

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.common.operators.Order

import org.apache.flink.api.common.functions.GroupReduceFunction
import scala.collection.JavaConverters._
import java.lang.Iterable

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

    val numSample = input.map { s => (s._1, 1) }.groupBy(0).sum(1)

    val adjacencySamples = samples
      .groupBy("label").reduceGroup(new GroupReduceFunction[Sample, AdjacencySample] {
        override def reduce(values: Iterable[Sample], out: Collector[AdjacencySample]): Unit = {
          var outputId = 0.0
          val outputList = values.asScala map { t => outputId = t.label; new Histo(t.feature, 1) }
          out.collect(new AdjacencySample(outputId, outputList))
        }
      })

    val sortedSample = numSample.join(adjacencySamples).where(0).equalTo("label") {
      (num, adjacenct, out: Collector[AdjacencySample]) =>
        val label = adjacenct.label
        val features = adjacenct.features.toList.sortBy(-_.value) //descending

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

        out.collect(new AdjacencySample(label, newFea))
    }

    //    val histo = numSample.join(adjacencySamples).where(0).equalTo("label") {
    //      (num, adjacenct, out: Collector[AdjacencySample]) =>
    //        val label = adjacenct.label
    //        val features = adjacenct.features.toList.sortBy(-_.value)
    //        println(" -----------------features----------------" + features(0))
    //        
    //                val finalFea = features.iterate(features.length-numBins){
    //                  currentFea =>
    //                    val newFea = currentFea
    //                }
    //
    //        out.collect(new AdjacencySample(label, features))
    //    }

    // emit result
    sortedSample.writeAsCsv(outputPath, "\n", "|")

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
      System.err.println("This program expects data from the TPC-H benchmark as input data.\n")
      false
    }
  }

  private def getDataSet(env: ExecutionEnvironment): DataSet[(Double, Double)] = {
    println(" start input")
    env.readCsvFile[(Double, Double)](inputPath, fieldDelimiter = " ", lineDelimiter = "\n",
      includedFields = Array(0, 1))
  }

}

//0.0|List(Histo(45.0,1.0), Histo(36.0,1.0), Histo(32.0,1.0), Histo(30.0,1.0), Histo(23.0,1.0), Histo(19.0,1.0), Histo(16.0,1.0), Histo(9.5,2.0), Histo(2.0,1.0))
//1.0|List(Histo(90.0,1.0), Histo(87.0,1.0), Histo(83.0,1.0), Histo(78.0,1.0), Histo(72.0,1.0), Histo(64.0,1.0), Histo(53.0,1.0), Histo(46.0,1.0), Histo(29.0,2.0))
