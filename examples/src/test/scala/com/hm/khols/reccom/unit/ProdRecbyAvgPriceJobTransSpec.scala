package com.hm.khols.reccom.unit

import com.twitter.scalding.TupleConversions
import com.pragmasoft.scaldingunit.TestInfrastructure
import org.specs2.{mutable => mutableSpec}
import com.twitter.scalding.RichPipe
import scala.collection.mutable
import com.hm.khols.reccom.{ProdReccomPipeTransformation,schemas}
import schemas._
import ProdReccomPipeTransformation._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
/**
 * @author wcbdd
 */
@RunWith(classOf[JUnitRunner])
class ProdRecbyAvgPriceJobTransSpec extends FlatSpec with ShouldMatchers with TupleConversions with TestInfrastructure  {
  
  //Test flatten of product list
  "A Reccom pipe transformation" should "flatten by product" in {

    Given {    
      List(("CLOTHING|FORMALS|SHIRT","111,222,777")) withSchema PROD_RECCOM_SCHEMA
    } When {
      pipe: RichPipe => pipe.getReccomByProd     

    } Then {
      buffer: mutable.Buffer[(String, String)] =>        
          buffer.toList  shouldEqual (List(("CLOTHING|FORMALS|SHIRT","111"),("CLOTHING|FORMALS|SHIRT","222"),("CLOTHING|FORMALS|SHIRT","777")))   
          buffer.toList.size ==  3
    }
  
  }
  
  "A product price pipe transformation" should "give avg price" in {

    Given {    
      List(("111","1500.21","1300.21")) withSchema PROD_PRICE_SCHEMA
    } When {
      pipe: RichPipe => pipe.calProdAvgPrice     

    } Then {
      buffer: mutable.Buffer[(String, String)] =>        
          buffer.toList  shouldEqual (List(("111","1400.21")))   
          
    }
  
  }
  
  "Top 2 recoomd " should "give top reccom based on avg price" in {

    Given {    
      List(("2150.227","111","CLOTHING|FORMALS|SHIRT"),("2500","222","CLOTHING|FORMALS|SHIRT")) withSchema PROD_AVGPRICE_SCHEMA
    } When {
      pipe: RichPipe => pipe.getTopProdsByAvgPrice(top = 2)     

    } Then {
      buffer: mutable.Buffer[(String, String)] =>        
          buffer.toList  shouldEqual (List(("CLOTHING|FORMALS|SHIRT","222,111")))   
          
    }
  
  }
  
}