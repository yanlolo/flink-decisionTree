package org.myorg.quickstart

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.common.operators.Order



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

    val sample = input.map{s => (s,1)}
    //sample(1).foreach(println)

   
    // emit result
    sample.writeAsCsv(outputPath, "\n", "|")

    // execute program
    env.execute(" Decision Tree ")
  }

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var inputPath: String = null
  private var outputPath: String = null
  case class Sample(label:Int, feature1:Double,feature2:Double)

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 2) {
      inputPath = args(0)
      outputPath = args(1)
      true
    } else {
      System.err.println("This program expects data from the TPC-H benchmark as input data.\n")
      false
    }
  }

  private def getDataSet(env: ExecutionEnvironment): DataSet[Sample] = {
        env.readCsvFile[Sample](inputPath, fieldDelimiter = ' ', lineDelimiter = "\n",
            includedFields = Array(0, 1, 2))
  }
  

}



/*
 * Output
Sample(0,23.0,10.0)|1
Sample(0,19.0,11.0)|1
Sample(0,10.0,12.0)|1
Sample(0,16.0,13.0)|1
Sample(0,36.0,14.0)|1
Sample(1,46.0,80.0)|1
Sample(1,78.0,81.0)|1
Sample(1,83.0,82.0)|1
Sample(1,30.0,83.0)|1
Sample(1,64.0,84.0)|1
Sample(0,2.0,15.0)|1
Sample(0,9.0,16.0)|1
Sample(0,32.0,17.0)|1
Sample(0,30.0,18.0)|1
Sample(0,45.0,19.0)|1
Sample(1,28.0,85.0)|1
Sample(1,87.0,86.0)|1
Sample(1,90.0,87.0)|1
Sample(1,53.0,88.0)|1
Sample(1,72.0,89.0)|1
 */



