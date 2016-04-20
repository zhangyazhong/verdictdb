package edu.umich.verdict.expr

import java.io.{PrintWriter, File}
import java.sql.ResultSet

import edu.umich.verdict.Configuration
import edu.umich.verdict.cli.ResultWriter
import edu.umich.verdict.connectors.DbConnector
import edu.umich.verdict.transformation.Parser

import scala.io.Source

class ErrorEstimationAccuracy() {
  var conf = new Configuration(new File(this.getClass.getClassLoader.getResource("expr/config.conf").getFile))
  var connector = DbConnector.createConnector(conf)
  var nSamples = 100
  var sampleSize = 0.01
  var table = "lineitem40"
  var nPoissonCols = 100
  var queries = Array(
    """
      |select
      |sum(quantity) as sum_qty,
      |sum(extendedprice) as sum_base_price,
      |avg(extendedprice) as avg_price,
      |count(*) as count_order
      |from
      |lineitem40
      |where
      |shipdate <= '1998-09-01'
      |and returnflag = 'A'
      |and linestatus = 'F'
    """.stripMargin)
  var exacts: Array[Array[Double]] = null
  var approxes: Array[Array[Array[ApproxResult]]] = null

  def execute(q: String): ResultSet = {
    Parser.parse(q).run(conf, connector)
  }

  def sampleName(i: Int) = s"error_test_${table}_s$i"

  def createSamples(): Unit = {
    for (i <- 1 to nSamples) {
      try {
        execute(s"create sample ${sampleName(i)} from $table with size $sampleSize% store $nPoissonCols poisson columns")
      } catch {
        case e: Exception => println(e.getMessage)
      }
    }
  }

  def removeSamples(): Unit = {
    for (i <- 1 to nSamples) {
      execute(s"drop sample ${sampleName(i)}")
    }
  }

  def runExacts() = {
    execute("set approximation = off")
    queries.zipWithIndex.foreach(q => {
      val pw = new PrintWriter(new File(s"error-test/${q._2}/exact"))
      ResultWriter.writeResultSet(pw, execute(q._1))
      pw.close()
    })
  }

  def runApproximates() = {
    execute("set approximation = on")
    queries.zipWithIndex.foreach(q => {
      val pw = new PrintWriter(new File(s"error-test/${q._2}/approx"))
      for (i <- 1 to nSamples) {
        execute(s"set bootstrap.fixed_sample = ${sampleName(i)}")
        ResultWriter.writeResultSet(pw, execute(q._1))
      }
      pw.close()
    })
  }

  def loadExacts() = {
    exacts = queries.indices.map(i => {
      Source.fromFile(s"error-test/$i/exact").getLines().toArray.apply(2).split("\\|").map(_.trim.toDouble)
    }).toArray
    exacts
  }

  def loadApproxes() = {
    if(exacts == null)
      loadExacts()
    approxes = queries.indices.map(q => {
    val nCols = exacts(q).length
      var lines = Source.fromFile(s"error-test/$q/approx").getLines().toArray
      lines = lines.indices.filter(_ % 3 == 2).map(lines).toArray
      (0 until nCols).map(i => {
        lines.map(line => {
          val vals = line.split("\\|").map(_.trim.toDouble)
          ApproxResult(vals(i), vals(i + nCols), vals(i + nCols * 2), vals(i + nCols * 3), vals(i + nCols * 4))
        })
      }).toArray
    }).toArray
    approxes
  }

  def main(args: Array[String]) {
    createDirs()

    println("Running Exacts ...")
    runExacts()

    println("Creating Samples ...")
    createSamples()

    println("Creating Approximates ...")
    execute(s"set bootstrap.sample_size = $sampleSize%")
    runApproximates()

    loadExacts()
    loadApproxes()

    println("Removing Samples ...")
    removeSamples()
  }

  def createDirs(): Unit = {
    new File("error-test").mkdir()
    queries.indices.foreach(i => {
      new File(s"error-test/$i").mkdir()
      val pw = new PrintWriter(new File(s"error-test/$i/q"))
      pw.println(queries(i))
      pw.close()
    })
  }

  case class ApproxResult(value: Double, ciLow: Double, ciHigh: Double, error: Double, variance: Double)

}

/*
Shell commands:

import edu.umich.verdict.expr._
var etest = new ErrorEstimationAccuracy()


 */