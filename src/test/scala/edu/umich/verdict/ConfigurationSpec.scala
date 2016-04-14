package edu.umich.verdict

import java.io.File

import org.scalatest.BeforeAndAfterAll

class ConfigurationSpec extends VerdictFlatSpec with BeforeAndAfterAll {
  var conf: Configuration = _

  override def beforeAll() {
    conf = new Configuration(new File(this.getClass.getClassLoader.getResource("testconfig.conf").getFile))
  }

  "Configuration" should "have the default values" in {
    conf.get("approximation") should not be null
  }

  it should "have values from file" in {
    conf.get("key1") shouldBe "Value1"
    conf.get("bootstrap.method") shouldBe "other"
  }

  it should "set values" in {
    conf.set("testKey", "testValue")
    conf.get("testKey") shouldBe "testValue"
  }

  it should "ignore key letter case" in {
    conf.set("TESTKEY2", "testValue3")
    conf.get("testkey2") shouldBe "testValue3"
  }

  it should "work well with numerical values" in {
    conf.set("int", "12")
    conf.set("double", "12.5")
    conf.getInt("int") shouldBe 12
    conf.getDouble("double") shouldBe 12.5
  }

  it should "work well with boolean values" in {
    conf.set("bool1", "False")
    conf.set("bool2", "OFF")
    conf.set("bool3", "no")
    conf.set("bool4", "TRUE")
    conf.set("bool5", "ON")
    conf.set("bool6", "yeS")
    conf.getBoolean("bool1") shouldBe false
    conf.getBoolean("bool2") shouldBe false
    conf.getBoolean("bool3") shouldBe false
    conf.getBoolean("bool4") shouldBe true
    conf.getBoolean("bool5") shouldBe true
    conf.getBoolean("bool6") shouldBe true
  }

  it should "work well with percentages" in {
    conf.set("percent1", ".546")
    conf.set("percent2", "3.12%")
    conf.getPercent("percent1") shouldBe .546
    conf.getPercent("percent2") shouldBe 3.12 / 100
  }

}
