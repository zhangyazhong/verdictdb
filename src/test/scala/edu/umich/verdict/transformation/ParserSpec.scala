package edu.umich.verdict.transformation

import edu.umich.verdict.VerdictFlatSpec
import edu.umich.verdict.processing._

class ParserSpec extends VerdictFlatSpec{
  "Parser" should "return a SelectStatement for SELECT queries" in {
    Parser.parse("select count(*) from t") shouldBe a [SelectStatement]
    Parser.parse("select * from t") shouldBe a [SelectStatement]
  }

  it should "return a ConfigStatement for SET and GET statements" in {
    Parser.parse("get sample_size") shouldBe a [ConfigStatement]
    Parser.parse("set approximation = off") shouldBe a [ConfigStatement]
  }

  it should "return a CreateSampleStatement for CREATE SAMPLE" in {
    Parser.parse("create sample s from t with size 10%") shouldBe a [CreateSampleStatement]
    Parser.parse("create sample s from t with size 10% store 10 poisson columns") shouldBe a [CreateSampleStatement]
    Parser.parse("create sample s from t with size 10% store 10 poisson columns stratified by c1, c2") shouldBe a [CreateSampleStatement]
    Parser.parse("create sample s from t with size 10% stratified by c1, c2") shouldBe a [CreateSampleStatement]
  }

  it should "return a DeleteSampleStatement for DELETE SAMPLE" in {
    Parser.parse("delete sample s") shouldBe a [DeleteSampleStatement]
  }

  it should "return a ShowSamplesStatement for SHOW SAMPLES" in {
    Parser.parse("show samples") shouldBe a [ShowSamplesStatement]
    Parser.parse("show all samples") shouldBe a [ShowSamplesStatement]
    Parser.parse("show uniform samples") shouldBe a [ShowSamplesStatement]
    Parser.parse("show stratified samples") shouldBe a [ShowSamplesStatement]
    Parser.parse("show stratified samples for t") shouldBe a [ShowSamplesStatement]
  }

  it should "return a ParsedStatement for other statements" in {
    Parser.parse("create table t") shouldBe a [ParsedStatement]
  }
}
