import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils


  object decitionTree{
    def main(args: Array[String]) {
	val conf = new SparkConf().setMaster("local").setAppName("My App")
	val sc = new SparkContext("local", "My App")
	val data = MLUtils.loadLibSVMFile(sc, "/home/hadoop/Desktop/test/past/4Spark/out")
	// Split the data into training and test sets (30% held out for testing)
	val splits = data.randomSplit(Array(1, 1))
	val (trainingData, testData) = (splits(0), splits(1))

	// Train a DecisionTree model.
	//  Empty categoricalFeaturesInfo indicates all features are continuous.
	val numClasses = 2
	val categoricalFeaturesInfo = Map[Int, Int]()
	val impurity = "gini"
	val maxDepth = 5
	val maxBins = 32

	val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
  impurity, maxDepth, maxBins)

	// Evaluate model on test instances and compute test error
	val labelAndPreds = testData.map { point =>
  		val prediction = model.predict(point.features)
  		(point.label, prediction)
	}

	val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
	println("Test Error = " + testErr)
	//println("Learned classification tree model:\n" + model.toDebugString)
      	sc.stop()
        println("this system exit ok!!!")
   }
  }  