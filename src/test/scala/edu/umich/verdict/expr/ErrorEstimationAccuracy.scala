package edu.umich.verdict.expr

import java.io.{PrintWriter, File}
import java.sql.ResultSet

import edu.umich.verdict.Configuration
import edu.umich.verdict.cli.ResultWriter
import edu.umich.verdict.connectors.DbConnector
import edu.umich.verdict.transformation.Parser

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
      |shipdate <= date '1998-09-01'
      |and returnflag = 'A'
      |and linestatus = 'F'
    """.stripMargin)

  def execute(q: String): ResultSet = {
    Parser.parse(q).run(conf, connector)
  }

  def sampleName(i: Int) = s"error_test_${table}_s$i"

  def createSamples(): Unit = {
    for(i <- 1 to nSamples){
      execute(s"create sample ${sampleName(i)} from $table with size $sampleSize% store $nPoissonCols poisson columns")
    }
  }

  def removeSamples(): Unit = {
    for(i <- 1 to nSamples){
      execute(s"drop sample ${sampleName(i)}")
    }
  }

  def runExacts() = {
    execute("set approximation = off")
    queries.zipWithIndex.foreach(q=>{
      val pw = new PrintWriter(new File(s"error-test/exact${q._2}"))
      ResultWriter.writeResultSet(pw, execute(q._1))
      pw.close()
    })
  }

  def runApproxomates() = {
    execute("set approximation = on")
    queries.zipWithIndex.foreach(q=>{
      val pw = new PrintWriter(new File(s"error-test/approx${q._2}"))
      for(i <- 1 to nSamples) {
        execute(s"set bootstrap.fixed_sample = ${sampleName(i)}")
        ResultWriter.writeResultSet(pw, execute(q._1))
      }
      pw.close()
    })
  }

  def main(args: Array[String]) {
    createDirs()

    println("Running Exacts ...")
    runExacts()

    println("Creating Samples ...")
    createSamples()

    println("Creating Approximates ...")
    execute(s"set bootstrap.sample_size = $sampleSize%")
    runApproxomates()

    println("Removing Samples ...")
    removeSamples()
  }

  def createDirs(): Unit = {
    new File("error-test").mkdir()
    queries.indices.foreach(i => new File(s"error-test/$i").mkdir())
  }
}
