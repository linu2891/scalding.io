package com.hm.khols.reccom.functional

import com.twitter.scalding.TupleConversions
import com.pragmasoft.scaldingunit.TestInfrastructure
import scala.collection.mutable
import com.hm.khols.reccom.ProdRecByPriceJob
import com.hm.khols.reccom.schemas._

import com.hm.khols.reccom.test.data.TestData._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.twitter.scalding.JobTest
import com.twitter.scalding.Csv
import com.twitter.scalding.Tsv
import com.hm.khols.reccom.ProdRecByPriceJob
/**
 * Functional test case
 * @author wcbdd
 */
@RunWith(classOf[JUnitRunner])
class ProdRecbyAvgPriceJobTest extends FlatSpec with ShouldMatchers with TupleConversions with TestInfrastructure {
  
    "Prod Recccom job" should "do the full transformation" in {

    JobTest(classOf[ProdRecByPriceJob].getName)
      .arg("prodRecommInput", "prodRecommInput")
      .arg("prodPriceInput", "prodPriceInput")
      .arg("output", "output")
      .arg("errorReccomRecords", "errorReccomRecords")
      .arg("errorPriceRecords", "errorPriceRecords")
      .source(Csv("prodRecommInput", ",", PROD_RECCOM_SCHEMA), prodReccomData)
      .source(Csv("prodPriceInput", ",", PROD_PRICE_SCHEMA), prodPriceData)
      
      .sink(Tsv("output")) {
        buffer: mutable.Buffer[(String, String)] =>
          buffer.toList shouldEqual prodReccomResult
      }
      .sink(Tsv("errorReccomRecords")) {
        buffer: mutable.Buffer[(String, String)] =>
            buffer.toList shouldEqual List()
      }      
      .sink(Tsv("errorPriceRecords")) {
        buffer: mutable.Buffer[(String, String)] =>
          buffer.toList.size == 1
      }
      .run
  }
  
}