package test
import math._

object test {
  case class Histo(featureValue: Double, frequency: Double)
  case class Histogram(label: Double, featureIndex: Int, histo: List[Histo])
  case class MergedHisto(featureIndex: Int, histo: List[Histo])
  case class Uniform(featureIndex: Int, uniform: List[Double])
  case class LabeledVector(label: Double, feature: List[Double])
  private val numSplit = 3 //By default it should be same as numBins

  def main(args: Array[String]) {
    val total = 1483
    //val test = new MergedHisto(0, List(Histo(5.981256890848953, 907.0), Histo(7.311111111111111, 45.0), Histo(7.316666666666666, 120.0), Histo(8.385826771653546, 381.0), Histo(8.666666666666666, 30.0)))
    //val test = new MergedHisto(1, List(Histo(5.0, 1.0), Histo(33.01587301587302, 63.0), Histo(118.11082251082252, 1155.0), Histo(120.18076923076923, 260.0), Histo(581.0, 4.0)))   
    //val test = new MergedHisto(2,List(Histo(18.357142857142858,14.0), Histo(23.642201834862384,218.0), Histo(34.32551319648094,682.0), Histo(52.75,4.0), Histo(75.10619469026548,565.0)))
    //val test = new MergedHisto(3,List(Histo(6.0,2.0), Histo(7.684210526315789,38.0), Histo(10.257907542579078,411.0), Histo(10.91891891891892,74.0), Histo(11.54070981210856,958.0)))
    //val test = new MergedHisto(4,List(Histo(501.5,2.0), Histo(939.3846153846154,13.0), Histo(984.4615384615385,13.0), Histo(1284.9361851332399,1426.0), Histo(1567.5172413793102,29.0)))
    //val test = new MergedHisto(5,List(Histo(1.0,1.0), Histo(23.0,1.0), Histo(51.18181818181818,11.0), Histo(54.75,4.0), Histo(81.01159618008185,1466.0)))
    //val test = new MergedHisto(6,List(Histo(38.17391304347826,69.0), Histo(43.0,2.0), Histo(66.44838212634822,649.0), Histo(68.71390728476821,755.0), Histo(90.125,8.0)))
    //val test = new MergedHisto(7, List(Histo(5.0, 2.0), Histo(10.0, 4.0), Histo(16.14917127071823, 181.0), Histo(20.629543696829078, 1293.0), Histo(28.333333333333332, 3.0)))
    //val test = new MergedHisto(8, List(Histo(7.0, 1.0), Histo(186.51369863013696, 146.0), Histo(222.375, 48.0), Histo(266.79185022026434, 908.0), Histo(281.386842105263, 380.0)))
  //val test = new MergedHisto(9,List(Histo(0.6983240223463687,895.0), Histo(0.8780487804878049,41.0), Histo(0.9522900763358778,524.0), Histo(1.0,22.0), Histo(1.0,1.0)))
    //val test = new MergedHisto(10,List(Histo(6.913617886178862,984.0), Histo(7.6,55.0), Histo(8.0,1.0), Histo(8.575000000000001,440.0), Histo(9.666666666666666,3.0)))
    //val test = new MergedHisto(11,List(Histo(0.0,1.0), Histo(1.0260869565217392,115.0), Histo(1.475,440.0), Histo(1.506493506493507,924.0), Histo(9.666666666666666,3.0)))
    val test = new MergedHisto(12,List(Histo(5.0,2.0), Histo(8.758454106280194,207.0), Histo(11.136363636363637,242.0), Histo(14.3904576436222,1027.0), Histo(23.4,5.0)))
    
    val histo = test.histo.toList.sortBy(_.featureValue) //ascend
    val len = histo.length
    var u = new Array[Double](numSplit - 1)

    for (j <- 1 to numSplit - 1) {
      var s = j * total / numSplit.toDouble

      if (s <= histo(0).frequency) {
        u(j - 1) = histo(0).featureValue
      } else {
        val totalSum = (0 until len) map { index => histo(index).frequency } reduce { _ + _ }
        val extendSum = totalSum - histo(len - 1).frequency / 2
        if (s >= extendSum) {
          u(j - 1) = histo(len - 1).featureValue
        } else {
          var i = 0
          var sumP = 0.0
          //sumPro
          while (sumP < s) {
            if (i == 0)
              sumP += histo(i).frequency / 2
            else
              sumP += histo(i).frequency / 2 + histo(i - 1).frequency / 2
            i += 1
          }
          i -= 2

          var d = s - (sumP - histo(i + 1).frequency / 2 - histo(i).frequency / 2)
          var a = histo(i + 1).frequency - histo(i).frequency
          var b = 2 * histo(i).frequency
          var c = -2 * d
          var z = if (a == 0) -c / b else (-b + sqrt(pow(b, 2) - 4 * a * c)) / (2 * a)

          u(j - 1) = histo(i).featureValue + (histo(i + 1).featureValue - histo(i).featureValue) * z
        }
      }
    }

    println(u.toList)

  }

}