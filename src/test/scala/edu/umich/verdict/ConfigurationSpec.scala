package edu.umich.verdict

import java.io.File

class ConfigurationSpec extends VerdictFlatSpec {
  val conf = new Configuration(new File(this.getClass.getClassLoader.getResource("testconfig.conf").getFile))

  "Configuration" should "have the default values" in {
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

  it should "work well with numerical values" in {
    conf.set("int", "12")
    conf.set("double", "12.5")
    assertResult(12){
      conf.getInt("int")
    }
    assertResult(12.5){
      conf.getDouble("double")
    }
  }

  it should "work well with boolean values" in {
    conf.set("bool1", "False")
    conf.set("bool2", "OFF")
    conf.set("bool3", "no")
    conf.set("bool4", "TRUE")
    conf.set("bool5", "ON")
    conf.set("bool6", "yeS")
    assertResult(false){
      conf.getBoolean("bool1")
    }
    assertResult(false){
      conf.getBoolean("bool2")
    }
    assertResult(false){
      conf.getBoolean("bool3")
    }
    assertResult(true){
      conf.getBoolean("bool4")
    }
    assertResult(true){
      conf.getBoolean("bool5")
    }
    assertResult(true){
      conf.getBoolean("bool6")
    }
  }

  it should "work well with percentages" in {
    conf.set("percent1", ".546")
    conf.set("percent2", "3.12%")
    assertResult(.546){
      conf.getPercent("percent1")
    }
    assertResult(3.12/100){
      conf.getPercent("percent2")
    }
  }

}
