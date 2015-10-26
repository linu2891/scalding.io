
package com.hm.khols.reccom

import com.twitter.scalding.Args
import com.twitter.scalding._
import com.twitter.scalding.Job
import cascading.pipe.Pipe
import UtilsConstant._




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
 







class ProdRecByPriceJob (args:Args) extends Job(args){

import schemas._
import ProdReccomPipeTransformation._        
   
  
  val recomPipe : Pipe =     Csv( args("prodRecommInput"),"," ,PROD_RECCOM_SCHEMA ).read .addTrap(Tsv( args("errorReccomRecords")))
  .getReccomByProd
                                                                   
 
  
  val prodPricePipe  : Pipe = Csv( args("prodPriceInput"),"," ,PROD_PRICE_SCHEMA ).read                                                     

  .calProdAvgPrice  .addTrap(Tsv( args("errorPriceRecords")))   
   
     
  .addReccomsToProducts(recomPipe)
 
  .getTopProdsByAvgPrice(TOP) 
    
 
  .write(Tsv( args("prodRecByPriceOutput") ))
      
 
}



   