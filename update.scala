import scala.io.Source
import collection.mutable.ArrayBuffer

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

        var histo = ArrayBuffer[Array[Double]]()
        val numBins = 5  // B bins for Update procedure
        var numMerge = 0 // which 2 close bins to merge  
       
             
        for(i<-0 to nums.size-1){
          if(i < numBins){
            histo += Array(nums(i).toDouble,1)
          }else{           
            histo += Array(nums(i).toDouble,1)
            histo = histo.sortWith(_(0)>_(0))
            
            // Find the closest 2 interval
            var min =  Integer.MAX_VALUE.toDouble  // interval of 2 bins
            for(j<-0 to histo.size-2){
              if( histo(j)(0).toDouble - histo(j+1)(0).toDouble < min){
                min =  histo(j)(0).toDouble - histo(j+1)(0).toDouble
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
        
      }     
  }  
}