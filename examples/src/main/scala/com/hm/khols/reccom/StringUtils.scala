

package com.hm.khols.reccom/**

import scala.util.control.Exception.catching
 * @author wcbdd
 */
object StringUtils {
  
  implicit class StringImprovements(val s: String) {
         import scala.util.control.Exception._
         def toIntOpt = catching(classOf[NumberFormatException]) opt s.toInt
         def toDoubleOpt = catching(classOf[NumberFormatException]) opt s.toDouble
         
         
     }
  
  def toDouble(s: String): Double = {
  try {
    s.toDouble 
    
  } catch {
    case e: NumberFormatException => 0
  }
}
}