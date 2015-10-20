
package com.hm.khols.reccom

import com.twitter.scalding.Args
import com.twitter.scalding._
import com.twitter.scalding.Job
import cascading.pipe.Pipe


/**
 * @author wcbdd
 * 
 * Re-rank the category-based recommendations based on price 
 * (defined as avg. of min/max either one nullable) asc., 
 * and retain only upto top N (say, N = 3) recommendations.
 * 
 * recomm    = "CLOTHING|FORMALS|SHIRT",   "111,222,777"           
 * prodPrice = 111, 1500,1200
 */
 
 object UtilsConstant {
  val PidOfProd = "PidOfProd"
  val MaxPrice = "maxprice"
  val MinPrice = "minprice"
  val AvgPrice = "avgPrice"
  val Category = "category"
  
  val MALE = "MALE"
  val TOP = 3;
}

 import UtilsConstant._
 package object schemas {
   
   val PROD_PRICE_SCHEMA = List(PidOfProd,MaxPrice, MinPrice)
   val PROD_CATALOG_SCHEMA = List ('pid, 'brand,'style,'gender,'typ1,'typ2,'typ3,'color)
   val PROD_RECCOM_SCHEMA = List(Category, 'poducts)
   val PROD_AVGPRICE_CAT_SCHEMA = List(AvgPrice, PidOfProd,Category) 
   val PROD_BY_CAT_SCHEMA = List('pid,'typ1,'typ2,'typ3)
   
   val RECCOM_BY_PRODUCT_SCHEMA = List('pid,'products)
   val OUTPUT_SCHEMA = List(Category, 'poducts) 
 
   
   
}


trait ProdReccomPipeTransformation {
  
  
  
import com.twitter.scalding.{Dsl, RichPipe}

 import scala.language.implicitConversions
 import Dsl._
 import StringUtils._
 import schemas._
  
  def pipe: Pipe
  
/**          
 *
 * flatten products list 
 *
 * INPUT_SCHEMA: PROD_RECCOM_SCHEMA
 * OUTPUT_SCHEMA: RECCOM_BY_PRODUCT_SCHEMA
 */
 def getReccomByProd : Pipe =
  pipe 
  .flatMap('poducts -> 'pidRecomm){recommednedProdLst:String => recommednedProdLst.split(",")}
  .project(Category,'pidRecomm)
  
  
 /**
 * Calculates the average price -  average price -> ( maxPrice  + minPrice ) / 2
 * 
 * INPUT_SCHEMA: PROD_PRICE_SCHEMA
 * OUTPUT_SCHEMA: PROD_AVG_PRICE_SCHEMA
 */
 def calProdAvgPrice : Pipe =
  pipe        
  .map((MaxPrice, MinPrice)->(AvgPrice)) {x:(String,String) => val(maxPrice,minPrice) = x 
    ((( toDouble(maxPrice) + toDouble(minPrice))/2))   
    }.project(PidOfProd,AvgPrice)
    
  
  /**
     * Joins with reccom schema to add category to avgprice
     *
     * Input schema: PROD_PRICE_SCHEMA
     * Recomm schema: RECCOM_SCHEMA
     * Output schema: PROD_AVGPRICE_CAT_SCHEMA
     */
    def addReccomsToProducts(reccomPipe: Pipe) = 
      pipe.joinWithSmaller(PidOfProd -> 'pidRecomm,  reccomPipe ).project(PROD_AVGPRICE_CAT_SCHEMA)   

       
  
/**          
 * Sort products by average price and and retain only upto top N
 * and create new reccom from avg price  
 * INPUT_SCHEMA: PROD_RECCOM_SCHEMA
 * OUTPUT_SCHEMA: RECCOM_BY_PRODUCT_SCHEMA
 */
 def getTopProdsByAvgPrice(top: Int) : Pipe =
  pipe 
    
   .groupBy(Category) { _.sortedReverseTake[(Double,String)](( AvgPrice,PidOfProd) -> 'top, top) } 
   .map('top -> 'pidList){ topList : List[(Double,String)] => topList.foldLeft("")((accum,tuple) => if(accum.isEmpty())tuple._2; else accum +","+tuple._2 )}
   .project(Category,'pidList)
     
 /**          
 * 
 *
 * INPUT_SCHEMA: prodCatalogSchema
 * OUTPUT_SCHEMA: PROD_RECCOM_BY_PROD_SCHEMA
 */
   def getPidCategoryByGender(gender:String): Pipe =
  pipe 
  .filter('gender){ f:String => f == gender}
   .project(PROD_BY_CAT_SCHEMA) 
    
   
 /**          
 * 
 *
 * INPUT_SCHEMA: prodCatalogSchema
 * OUTPUT_SCHEMA: prodCatalogSchema
 */
   def getCategoryFromCatalog: Pipe =
  pipe
  .project(PROD_BY_CAT_SCHEMA)
  .map( ('typ1,'typ2,'typ3) -> 'category_){x:(String,String,String) => val(typ1,typ2,typ3) = x  //create category "typ1|typ2|typ3"
    s"$typ1|$typ2|$typ3"}
   .discard('typ1,'typ2,'typ3)

   /**
    * 
    */
  def removeSelfRecomm: Pipe =

    pipe
      .project(RECCOM_BY_PRODUCT_SCHEMA)
      .map((RECCOM_BY_PRODUCT_SCHEMA) -> (RECCOM_BY_PRODUCT_SCHEMA)) { x: (String, String) =>
        val (pid, poducts) = x
        filterProducts(pid, poducts)
      }

   /**
    * pid : product Id
    * products : reccomended products for pid
    * 
    *  remove self reccom from products
    */
  def filterProducts(pid: String, products: String): (String, String) = {
    if (products.contains(pid)) {
      var prodArray = products.split(",")
      val b = prodArray.filter(!_.contains(pid))

      val newProducts = b.mkString(",")
      (pid, newProducts)
    } else
      (pid, products)
  }
  
}


object ProdReccomPipeTransformation  {
  implicit def wrapRicpPipe(rp: RichPipe): ProdReccomPipeTransformationWrapper = new ProdReccomPipeTransformationWrapper(rp.pipe)
  implicit class ProdReccomPipeTransformationWrapper(val pipe: Pipe) extends ProdReccomPipeTransformation with Serializable
}



class ProdRecByPriceJob (args:Args) extends Job(args){

import schemas._
import ProdReccomPipeTransformation._        
   
  
  val recomPipe : Pipe =     Csv( args("prodRecommInput"),"," ,PROD_RECCOM_SCHEMA ).read .addTrap(Tsv( args("errorReccomRecords")))
  .getReccomByProd
                                                                   
 
  
  val prodPricePipe  : Pipe = Csv( args("prodPriceInput"),"," ,PROD_PRICE_SCHEMA ).read                                                     

  .calProdAvgPrice  .addTrap(Tsv( args("errorPriceRecords")))   
   
     
  .addReccomsToProducts(recomPipe)
 
  .getTopProdsByAvgPrice(TOP) 
    
 
  .write(Tsv( args("output") ))
      
 
}



   