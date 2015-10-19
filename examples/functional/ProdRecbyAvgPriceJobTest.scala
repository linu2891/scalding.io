package com.hm.khols.reccom.functional

import com.twitter.scalding.TupleConversions
import com.pragmasoft.scaldingunit.TestInfrastructure
import scala.collection.mutable
import com.hm.khols.reccom.ProdRecByPriceJob
import com.hm.khols.reccom.schemas._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.twitter.scalding.JobTest
import com.twitter.scalding.Csv
import com.hm.khols.reccom.ProdRecByPriceJob
/**
 * @author wcbdd
 */
@RunWith(classOf[JUnitRunner])
class ProdRecbyAvgPriceJobTransSpec extends FlatSpec with ShouldMatchers with TupleConversions with TestInfrastructure {
  
    "A sample job" should "do the full transformation" in {

    JobTest(classOf[ProdRecByPriceJob].getName)
      .arg("prodRecommInput", "prodRecommInput")
      .arg("prodPriceInput", "prodPriceInput")
      .arg("output", "output")
      .source(Csv("prodRecommInput", ",", PROD_RECCOM_SCHEMA), List(("CLOTHING|FORMALS|SHIRT","111,222,777")))
      .source(Csv("prodPriceInput",",", PROD_PRICE_SCHEMA), List(111,1500,1300))
      .sink(Csv("outputPathFile",",", OUTPUT_SCHEMA)) {
          buffer: mutable.Buffer[(String, Long, String, String, Long)] =>
            buffer.toList shouldEqual List(("2013/02/11", 1000002l, "stefano@email.com", "10 Downing St. London", 1l))
        }
      .run
  }
  
}