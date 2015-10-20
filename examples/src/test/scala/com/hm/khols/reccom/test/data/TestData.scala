package com.hm.khols.reccom.test.data

/**
 * @author wcbdd
 */
class TestData {
  
}

object TestData{
  
  /**
   * Input For Jobs 
   */
  val prodReccomData = List(
                                  ("CLOTHING|FORMALS|SHIRT","111,222,777"),
                                  ("CLOTHING|FORMALS|TROUSERS","444,555,666"),
                                  ("FOOTWARE|CASUALS|SHOES","333")                            
                           )
                           
  val prodPriceData = List(
                                  ("111","1500.122","1300.21"),
                                  ("222", "1800","1400"),
                                  ("777","1200","1000"),
                                  ("444","1200","1050"),
                                  ("333","","888"),                            
                                  ("adc",12,11),
                                  ("adc","",11) 
                            
                           )
                           

  val prodCatalogData =  List(
                                  (111,"LEVIS","style","MALE","CLOTHING","FORMALS","SHIRT","BLUE"),
                                  (222,"LEVIS","style","MALE","CLOTHING","FORMALS","SHIRT","BLACK"),
                                  (333,"NIKE","style","MALE","FOOTWARE","CASUALS","SHOES","BLUE"),
                                  (444,"NIKE","style","FEMALE","FOOTWARE","CASUALS","SHOES","PINK"),
                                  (777,"LEVIS","style","MALE","CLOTHING","FORMALS","SHIRT","GREEN")                           
                                                              
                             )                       
                          
  /**
   * Output For Jobs                          
   */
  
  val prodReccomResult = List(
                                   ("CLOTHING|FORMALS|SHIRT", "222,111,777"),
                                   ("CLOTHING|FORMALS|TROUSERS", "444"),
                                   ("FOOTWARE|CASUALS|SHOES","333")
                              )
                              
  val prodReccomForMenByProdRes =  List(
                                   ("111", "222,777"),
                                   ("222", "111,777"),
                                   ("777", "111,222"),
                                   ("333", "")
                              )                        
}