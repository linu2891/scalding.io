package com.hm.khols.reccom


import com.twitter.scalding.Args
import com.twitter.scalding._
import com.twitter.scalding.Job
import cascading.pipe.Pipe



/**
 * For every Men's prolduct in the product catalog, 
 * come up with the list of recommended products based on category, 
 * and filter out any potential self-recommendations from it.
 * 
 * @author wcbdd
 */

 
  
class ReccomForMenByProd (args:Args) extends Job(args){

  import schemas._
  import ProdReccomPipeTransformation._  
  import UtilsConstant._
  
  val prodRecomPipe : Pipe = Csv( args("prodRecommInput"),"," ,PROD_RECCOM_SCHEMA ).read 
  
  val prodPipe : Pipe = Csv( args("prodCatalogInput"), ",", PROD_CATALOG_SCHEMA). read
  .getPidCategoryByGender(MALE)
  .getCategoryFromCatalog
  .joinWithSmaller('category_ -> 'category ,  prodRecomPipe)
  .removeSelfRecomm
  .write(Tsv( args("reccomByProdOutput")))
  
}
