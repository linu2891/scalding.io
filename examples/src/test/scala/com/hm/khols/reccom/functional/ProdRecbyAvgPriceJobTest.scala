package com.hm.khols.reccom.functional

import com.twitter.scalding.TupleConversions
import com.pragmasoft.scaldingunit.TestInfrastructure
import scala.collection.mutable
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.twitter.scalding.JobTest
import com.twitter.scalding.Csv
import com.twitter.scalding.Tsv
import com.hm.khols.reccom.ProdRecByPriceJob
/**
 * Functional test case for
 * Re-rank the category-based recommendations based on price 
 * (defined as avg. of min/max  either one nullable) asc., 
 * and retain only upto top N (say, N = 5) recommendations.
 * 
 * @author wcbdd
 */
@RunWith(classOf[JUnitRunner])
class ProdRecbyAvgPriceJobTest extends FlatSpec with ShouldMatchers with TupleConversions with TestInfrastructure {
  
  import com.hm.khols.reccom.ProdRecByPriceJob
  import com.hm.khols.reccom.schemas._
  import com.hm.khols.reccom.test.data.TestData._

    "Prod Recccom by AvgPrice job" should "do the full transformation" in {

    JobTest(classOf[ProdRecByPriceJob].getName)
      .arg("prodRecommInput", "prodRecommInput")
      .arg("prodPriceInput", "prodPriceInput")
      .arg("prodRecByPriceOutput", "prodRecByPriceOutput")
      .arg("errorReccomRecords", "errorReccomRecords")
      .arg("errorPriceRecords", "errorPriceRecords")
      .source(Csv("prodRecommInput", ",", PROD_RECCOM_SCHEMA), prodReccomData)
      .source(Csv("prodPriceInput", ",", PROD_PRICE_SCHEMA), prodPriceData)
      
      .sink(Tsv("prodRecByPriceOutput")) {
        buffer: mutable.Buffer[(String, String)] =>
          buffer.toList shouldEqual prodReccomResult
          //print the o/p on console
          println("\n\n")
          println("\t \t \t \t \t \t Input \n\n")            
          println("product price ")
          println(prodPriceData); println("\n\n" )
          println("reccomendations ")
          println(prodReccomData)
          println("\n\n\t \t \t \t \t \t Output \n\n")
          println("reccomendations by products based on price ")
          println(buffer.toList)
          
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
      .finish
  }
  
}