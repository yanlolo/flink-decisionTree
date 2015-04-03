package lolo

import scala.io.Source
import collection.mutable.ArrayBuffer
import math._



object DecisionTree {
  def main(args: Array[String]): Unit = {
      println("--Test for functions--")
      val source = Source.fromFile("d:/Userfiles/yyan/Desktop/data/test.txt")
      val lines = source.getLines
      
      for (line<-lines){
        println("---line---  "+line)         
        val numStr = line.toString
        println("num string  "+numStr)
        val nums = numStr.split(" ")
        nums.foreach(println)
        println("--nums size--"+nums.size)
        
        val numBins = 5  // B bins for Update procedure
        var histo = updatePro(nums, numBins)
        
        // Print out test result
        println(histo(0)(0), histo(0)(1))
        println(histo(1)(0), histo(1)(1))
        println(histo(2)(0), histo(2)(1)) 
        println(histo(3)(0), histo(3)(1))
        println(histo(4)(0), histo(4)(1)) 
        val sumTest = sumPro(histo,15)(0)
        val iTest = sumPro(histo,15)(1)
        println(sumTest)
        println(iTest)
        
        
      }
 }
 
 
 
   // Update procedure
  def updatePro(nums: Array[String], numBins:Int): ArrayBuffer[Array[Double]]={
        
    var numMerge = 0 // which 2 close bins to merge  
    var histo = ArrayBuffer[Array[Double]]()   
             
        for(i<-0 to nums.size-1){
          if(i < numBins){
            histo += Array(nums(i).toDouble,1)
          }else{           
            histo += Array(nums(i).toDouble,1)
            histo = histo.sortWith(_(0)<_(0))
            
            // Find the closest 2 interval
            var min =  Integer.MAX_VALUE.toDouble  // interval of 2 bins
            for(j<-0 to histo.size-2){
              if( histo(j+1)(0).toDouble - histo(j)(0).toDouble < min){
                min =  histo(j+1)(0).toDouble - histo(j)(0).toDouble
                numMerge = j               
              }                  
            }
         
            val newBinP = (histo(numMerge)(0)*histo(numMerge)(1)+histo(numMerge+1)(0)*histo(numMerge+1)(1))/(histo(numMerge)(1)+histo(numMerge+1)(1))
            val newBinK = histo(numMerge)(1)+histo(numMerge+1)(1)
            val newBin = Array(newBinP,newBinK)
               
            histo.remove(numMerge+1)
            histo.remove(numMerge)
            histo.insert(numMerge,newBin)
            
          }  
        }       
    histo
  }
  
  // Sum procedure
  def sumPro(histo: ArrayBuffer[Array[Double]], b:Double): Array[Double]={
    var i = 0 
    
    while (b>=histo(i)(0)){
      i+=1
    }
    i-=1
    
    val mi = histo(i)(1)
    val mii = histo(i+1)(1)
    val pi = histo(i)(0)
    val pii = histo(i+1)(0)
    val mb = mi+(mii-mi)*(b-pi)/(pii-pi)
    var s = (mi+mb)*(b-pi)/(2*(pii-pi))
    
    for (j<-0 to i-1){
      s+=histo(j)(1)
    }
    
    s+=histo(i)(1)/2
    val result = Array(s,i)
    result
  }
   
   
 
 
}
