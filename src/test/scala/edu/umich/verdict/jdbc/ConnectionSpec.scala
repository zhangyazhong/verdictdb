package edu.umich.verdict.jdbc

import java.io.File
import java.sql.{Connection, DriverManager}

import edu.umich.verdict.{Configuration, VerdictFlatSpec}
import org.scalatest.BeforeAndAfterAll

class ConnectionSpec extends VerdictFlatSpec with BeforeAndAfterAll {
  var connection: Connection = null

  override def beforeAll() {
    Class.forName("edu.umich.verdict.jdbc.Driver")
  }

  override def afterAll(): Unit = {
    if (connection != null)
      connection.close()
  }

  "JDBC Connection" should "connect to impala" in {
    connection = DriverManager.getConnection("jdbc:verdict:impala://bigdata.eecs.umich.edu:21050", new Configuration(new File(this.getClass.getClassLoader.getResource("impala.conf").getFile)).toProperties)
    connection should not be null
  }

  it should "habdle config file in URL" in {
    DriverManager.getConnection("jdbc:verdict:impala://bigdata.eecs.umich.edu:21050?config=" + this.getClass.getClassLoader.getResource("impala.conf").getPath) should not be null
  }
  //
  //  "JDBC Connection" should "connect to impala with correct schema" in {
  //    DriverManager.getConnection("jdbc:verdict:impala://bigdata.eecs.umich.edu:21050/verdict", new Configuration(new File(this.getClass.getClassLoader.getResource("impala.conf").getFile)).toProperties).getSchema shouldBe "verdict"
  //  }

  it should "run non-select statements via prepareStatement" in {
    connection.prepareStatement("show tables").executeQuery() should not be null
  }

  it should "run select statements via prepareStatement" in {
    connection.prepareStatement("select count(*) from web_logs").executeQuery() should not be null
  }

  it should "create statement" in {
    connection.createStatement() shouldBe a[VStatement]
  }
}
