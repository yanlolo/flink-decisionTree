package pageRank


import java.lang.Iterable

import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.examples.java.graph.util.PageRankData
import org.apache.flink.api.java.aggregation.Aggregations.SUM

import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

object PageRankBasic {

  private final val DAMPENING_FACTOR: Double = 0.85
  private final val EPSILON: Double = 0.0001

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // read input data
    val pages = getPagesDataSet(env)
    val links = getLinksDataSet(env)

    // assign initial ranks to pages
    val pagesWithRanks = pages.map(p => Page(p, 1.0 / numPages)).withForwardedFields("*->pageId")

    // build adjacency list from link input
    val adjacencyLists = links
      .groupBy("sourceId").reduceGroup( new GroupReduceFunction[Link, AdjacencyList] {
        override def reduce(values: Iterable[Link], out: Collector[AdjacencyList]): Unit = {
          var outputId = -1L
          val outputList = values.asScala map { t => outputId = t.sourceId; t.targetId }
          out.collect(new AdjacencyList(outputId, outputList.toArray))
        }
      })

    // start iteration
    val finalRanks = pagesWithRanks.iterateWithTermination(maxIterations) {
      currentRanks =>
        val newRanks = currentRanks
          // distribute ranks to target pages
          .join(adjacencyLists).where("pageId").equalTo("sourceId") {
            (page, adjacent, out: Collector[Page]) =>
              val targets = adjacent.targetIds
              val len = targets.length
              adjacent.targetIds foreach { t => out.collect(Page(t, page.rank /len )) }
          }
          // collect ranks and sum them up
          .groupBy("pageId").aggregate(SUM, "rank")
          // apply dampening factor
          .map { p =>
            Page(p.pageId, (p.rank * DAMPENING_FACTOR) + ((1 - DAMPENING_FACTOR) / numPages))
          }.withForwardedFields("pageId")

        // terminate if no rank update was significant
        val termination = currentRanks.join(newRanks).where("pageId").equalTo("pageId") {
          (current, next, out: Collector[Int]) =>
            // check for significant update
            if (math.abs(current.rank - next.rank) > EPSILON) out.collect(1)
        }
        (newRanks, termination)
    }

    val result = finalRanks

    // emit result
    if (fileOutput) {
      result.writeAsCsv(outputPath, "\n", " ")
    } else {
      result.print()
    }

    // execute program
    env.execute("Basic PageRank Example")
  }

  // *************************************************************************
  //     USER TYPES
  // *************************************************************************

  case class Link(sourceId: Long, targetId: Long)

  case class Page(pageId: Long, rank: Double)

  case class AdjacencyList(sourceId: Long, targetIds: Array[Long])

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 5) {
        pagesInputPath = args(0)
        linksInputPath = args(1)
        outputPath = args(2)
        numPages = args(3).toLong
        maxIterations = args(4).toInt

        true
      } else {
        System.err.println("Usage: PageRankBasic <pages path> <links path> <output path> <num " +
          "pages> <num iterations>")

        false
      }
    } else {
      System.out.println("Executing PageRank Basic example with default parameters and built-in " +
        "default data.")
      System.out.println("  Provide parameters to read input data from files.")
      System.out.println("  See the documentation for the correct format of input files.")
      System.out.println("  Usage: PageRankBasic <pages path> <links path> <output path> <num " +
        "pages> <num iterations>")

      numPages = PageRankData.getNumberOfPages

      true
    }
  }

  private def getPagesDataSet(env: ExecutionEnvironment): DataSet[Long] = {
    if (fileOutput) {
      env.readCsvFile[Tuple1[Long]](pagesInputPath, fieldDelimiter = " ", lineDelimiter = "\n")
        .map(x => x._1)
    } else {
      env.generateSequence(1, 15)
    }
  }

  private def getLinksDataSet(env: ExecutionEnvironment): DataSet[Link] = {
    if (fileOutput) {
      env.readCsvFile[Link](linksInputPath, fieldDelimiter = " ",
        includedFields = Array(0, 1))
    } else {
      val edges = PageRankData.EDGES.map { case Array(v1, v2) => Link(v1.asInstanceOf[Long],
        v2.asInstanceOf[Long])}
      env.fromCollection(edges)
    }
  }

  private var fileOutput: Boolean = false
  private var pagesInputPath: String = null
  private var linksInputPath: String = null
  private var outputPath: String = null
  private var numPages: Long = 0
  private var maxIterations: Int = 10

}