package com.hm.khols.reccom.unit

import com.twitter.scalding.TupleConversions
import com.pragmasoft.scaldingunit.TestInfrastructure
import org.specs2.{mutable => mutableSpec}
import com.twitter.scalding.RichPipe
import scala.collection.mutable
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

/**
 * @author wcbdd
 */
@RunWith(classOf[JUnitRunner])
class RecForMenByProdJobTransSpec extends FlatSpec with ShouldMatchers with TupleConversions with TestInfrastructure {

  import com.hm.khols.reccom.{ ProdReccomPipeTransformation, schemas, UtilsConstant }
  import schemas._
  import ProdReccomPipeTransformation._
  import UtilsConstant._
  
   "A Catalog pipe transformation" should "get all male products" in {

    Given {    
      List(("111","LEVIS","style","MALE","CLOTHING","FORMALS","SHIRT","BLUE") ,
           ("444","NIKE","style","FEMALE","FOOTWARE","CASUALS","SHOES","PINK") ) withSchema PROD_CATALOG_SCHEMA
    } When {
      pipe: RichPipe => pipe.getPidCategoryByGender(MALE)    

    } Then {
      buffer: mutable.Buffer[(String, String, String, String)] =>        
          buffer.toList  shouldEqual (List(("111","CLOTHING","FORMALS","SHIRT")))       
    }
  
  }
   
  "A Catalog pipe transformation" should "get product by category " in {

    Given {    
      List(("111","CLOTHING","FORMALS","SHIRT")) withSchema PROD_BY_CAT_SCHEMA
    } When {
      pipe: RichPipe => pipe.getCategoryFromCatalog     

    } Then {
      buffer: mutable.Buffer[(String, String)] =>        
          buffer.toList  shouldEqual (List(("111","CLOTHING|FORMALS|SHIRT")))       
    }
  
  }
  

  "A Reccom pipe transformation" should "remove all slef reccom" in {
    
     Given {    
      List(("111","222,777,111") ,
           ("222","111,222,777") ) withSchema RECCOM_BY_PRODUCT_SCHEMA
    } When {
      pipe: RichPipe => pipe.getReccomByProd     

    } Then {
      buffer: mutable.Buffer[(String, String)] =>        
          buffer.toList  shouldEqual (List( ("111","222,777"),(("222","111,777"))) )       
    }
  }
  
}