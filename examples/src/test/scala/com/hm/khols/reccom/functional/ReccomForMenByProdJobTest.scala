package com.hm.khols.reccom.functional


import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.twitter.scalding.JobTest
import com.twitter.scalding.Csv
import com.twitter.scalding.Tsv
import com.hm.khols.reccom.ProdRecByPriceJob
import com.twitter.scalding.TupleConversions
import com.pragmasoft.scaldingunit.TestInfrastructure
import scala.collection.mutable

/**
 * @author wcbdd
 */
@RunWith(classOf[JUnitRunner])
class ReccomForMenByProdJobTest extends FlatSpec with ShouldMatchers with TupleConversions with TestInfrastructure{
  
  
  import com.hm.khols.reccom.ReccomForMenByProd
  import com.hm.khols.reccom.schemas._
  import com.hm.khols.reccom.test.data.TestData._

    "Prod Recccom by products job" should "do the full transformation" in {

    JobTest(classOf[ReccomForMenByProd].getName)
      .arg("prodRecommInput", "prodRecommInput")
      .arg("prodCatalogInput", "prodCatalogInput")
      .arg("reccomByProdOutput", "reccomByProdOutput")   
      .source(Csv("prodRecommInput", ",", PROD_RECCOM_SCHEMA), prodReccomData)
      .source(Csv("prodCatalogInput", ",", PROD_CATALOG_SCHEMA), prodCatalogData)
      
      .sink(Tsv("reccomByProdOutput")) {
        buffer: mutable.Buffer[(String, String)] =>
          buffer.toList shouldEqual prodReccomForMenByProdRes        
          //output on console
          println("\n\n")
          println("\t \t \t \t \t \t Input \n\n")            
          println(prodReccomData); println("\n\n" )
          println(prodCatalogData)
          println("\n\n\t \t \t \t \t \t Output \n\n")
          println(buffer.toList)
      }      
      .run
      .finish
  }
  
}