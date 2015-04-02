
import scala.io.Source
import collection.mutable.ArrayBuffer
import math._

object DecisionTree {
  def main(args: Array[String]): Unit = {
  
      val source = Source.fromFile("d:/Userfiles/yyan/Desktop/data/test.txt")
      val lines = source.getLines
      
      for (line<-lines){
        println("---line---  "+line)         
        val numStr = line.toString
        println("num string  "+numStr)
        val nums = numStr.split(" ")
        nums.foreach(println)
        println("--nums size--"+nums.size)
        
        // Update procedure 
        var histo = ArrayBuffer[Array[Double]]()
        val numBins = 5  // B bins for Update procedure
        var numMerge = 0 // which 2 close bins to merge  
       
             
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
         
            var newBinP = (histo(numMerge)(0)*histo(numMerge)(1)+histo(numMerge+1)(0)*histo(numMerge+1)(1))/(histo(numMerge)(1)+histo(numMerge+1)(1))
            var newBinK = histo(numMerge)(1)+histo(numMerge+1)(1)
            var newBin = Array(newBinP,newBinK)
               
            histo.remove(numMerge+1)
            histo.remove(numMerge)
            histo.insert(numMerge,newBin)
            
          }  
        }
        
       println(histo(0)(0), histo(0)(1))
       println(histo(1)(0), histo(1)(1))
       println(histo(2)(0), histo(2)(1)) 
       println(histo(3)(0), histo(3)(1))
       println(histo(4)(0), histo(4)(1)) 
       
       
       // Uniform procedure
       val numSplit = 3  // Normally, B(hat) is equal to B (numBins)  ==> for test is 3
       var u = new Array[Double](numSplit-1)
      
       for(j<-1 to numSplit-1){
         var s = j*nums.size/numSplit.toDouble
         
         var i= 0
         var sumP = 0.0
         // Sum Procedure
         while (sumP<s){
           if (i==0)
             sumP += histo(i)(1)/2
           else 
             sumP += histo(i)(1)/2 + histo(i-1)(1)/2
           i+=1
         }
         i-=2
         
         var d = s-(sumP-histo(i+1)(1)/2-histo(i)(1)/2)
         var a = histo(i+1)(1)-histo(i)(1)
         var b = 2*histo(i)(1)
         var c = -2*d
         var z = if (a ==0 ) -c/b else (-b+sqrt(pow(b,2)-4*a*c))/(2*a)
         
         u(j-1) = histo(i)(0)+(histo(i+1)(0)-histo(i)(0))*z
         println("u("+j+")="+u(j-1))
       }
       
       
      }     
  }  
}