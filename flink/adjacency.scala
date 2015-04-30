package org.myorg.quickstart

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.common.operators.Order

import org.apache.flink.api.common.functions.GroupReduceFunction
import scala.collection.JavaConverters._

object WordCount {

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // get execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get customer data set: (custkey, name, address, nationkey, acctbal) 
    val input = getDataSet(env)

    val featureNum = 2 // number of independent features
    val numBins = 5 // B bins for Update procedure
    val numSplit = 3 //By default it should be same as numBins
    val numLevel = 3 // how many levels of tree
    val leastSample = 5 // least number of samples in one node
    println("-- Welcom to Decision Tree --")

    //val output = input.groupBy(1).aggregate(Aggregations.SUM, 0).and(Aggregations.MIN, 2)

    // val samples = input.map { s => (s._1, s._2, 1) }
    //   .groupBy(0)
    //   .reduce { (s1, s2) => (s1._1, s1._2 + s2._2, s1._3 + s2._3) }

    val samples = input.map { s => Sample(s._1, s._2) }

    val adjacencySamples = samples
      .groupBy("label").reduceGroup(new GroupReduceFunction[Sample, AdjacencySample] {
        override def reduce(values: Iterable[Sample], out: Collector[AdjacencySample]): Unit = {
          var outputId = 0
          val outputList = values.asScala map { t => outputId = t.label; t.feature }
          out.collect(new AdjacencySample(outputId, outputList.toArray))
        }
      })

    // emit result
    adjacencySamples.writeAsCsv(outputPath, "\n", "|")

    // execute program
    env.execute(" Decision Tree ")
  }

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var inputPath: String = null
  private var outputPath: String = null

  case class Sample(label: Int, feature: Double)
  case class AdjacencySample(label: Int, feature: Array[Double])

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

  private def getDataSet(env: ExecutionEnvironment): DataSet[(Int, Double)] = {
    env.readCsvFile[(Int, Double)](inputPath, fieldDelimiter = ' ', lineDelimiter = "\n",
      includedFields = Array(0, 1))
  }

}

