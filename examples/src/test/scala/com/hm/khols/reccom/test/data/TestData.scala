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
                          
  /**
   * Output For Jobs                          
   */
  
  val prodReccomResult = List(
                                   ("CLOTHING|FORMALS|SHIRT", "222,111,777"),
                                   ("CLOTHING|FORMALS|TROUSERS", "444"),
                                   ("FOOTWARE|CASUALS|SHOES","333")
                              )
}