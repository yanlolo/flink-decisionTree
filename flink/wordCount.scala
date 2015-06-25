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

    // set up the execution environment
    //val env = ExecutionEnvironment.getExecutionEnvironment
    val env = ExecutionEnvironment.createLocalEnvironment()

    //get input data
    val text = env.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")

    val counts = text.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    // emit result
    counts.print()

    // execute program
    env.execute("WordCount Example")
  }

}
