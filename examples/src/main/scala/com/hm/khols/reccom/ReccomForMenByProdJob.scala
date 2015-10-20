package com.hm.khols.reccom


import com.twitter.scalding.Args
import com.twitter.scalding._
import com.twitter.scalding.Job
import cascading.pipe.Pipe

/**
 * @author wcbdd
 */

 
  
class ReccomForMenByProd (args:Args) extends Job(args){

  import schemas._
  import ProdReccomPipeTransformation._  
  import UtilsConstant._
  
  val prodRecomPipe = Csv( args("prodRecommInput"),"," ,PROD_RECCOM_SCHEMA ).read 
  
  val prodPipe = Csv( args("prodCatalogInput"),"," ,PROD_CATALOG_SCHEMA ).read
  .getCategoryByGender(MALE)
  .getCategoryFromCatalog
  .joinWithSmaller('category_ -> 'category ,  prodRecomPipe)
  .removeSelfRecomm
  .write(Tsv( args("output")))
  
}
