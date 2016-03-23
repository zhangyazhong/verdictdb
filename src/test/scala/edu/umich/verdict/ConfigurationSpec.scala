package edu.umich.verdict

import java.io.File

import org.scalatest._

class ConfigurationSpec extends VerdictFlatSpec {
  val conf = new Configuration(new File(this.getClass.getClassLoader.getResource("testconfig.conf").getFile))

  "Configuration" should "have default values" in {
    assert(conf.get("bootstrap") != null)
  }

  it should "have values from file" in {
    assertResult("Value1"){
      conf.get("key1")
    }
    assertResult("other"){
      conf.get("bootstrap.method")
    }
  }

  it should "set values" in {
    conf.set("testKey", "testValue")
    assertResult("testValue"){
      conf.get("testKey")
    }
  }

  it should "ignore key letter case" in {
    conf.set("TESTKEY2", "testValue3")
    assertResult("testValue3"){
      conf.get("testkey2")
    }
  }
}
