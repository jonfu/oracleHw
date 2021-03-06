package oracle.recruitment.exercise

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.kohsuke.args4j.Option
import oracle.recruitment.util.{HdfsUtils, Options}


// These will be useful
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.DoubleRDDFunctions
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import scala.collection.JavaConversions._
import scala.io.Source


class MyOptions (args: Array[String]) extends Options {
	@Option(name = "-sm", usage = "Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).")
	var sparkMaster: String = "local"

	@Option(name = "-sn", usage = "Application name, to display on the cluster web UI.")
	var sparkApplicationName: String = "Exercise"

	@Option(name = "-sh", usage = "Spark home: Location where Spark is installed on cluster nodes.")
	var sparkHome: String = _

	var sparkJars: Seq[String] = _
	@Option(name = "-sj", usage = "Values are bar|delimited.  Collection of JARs to send to the cluster. " +
		"These can be paths on the local file system or HDFS, HTTP, HTTPS, or FTP URLs.")
	def setSparkJars(sparkJars: String) = this.sparkJars = sparkJars.split("\\|")

	@Option(name = "-i", usage = "HDFS url of input data." )
	var inputPath: String = _

	@Option(name = "-o", usage = "HDFS url of output data directory." )
	var outputPath: String = _

	def getSparkContext: SparkContext = {
		new SparkContext(sparkMaster, sparkApplicationName, sparkHome, sparkJars)
	}

	initialize ( args )
}



object Exercise {
  
	def main ( args: Array[String] ) {

		// options
		val options: MyOptions = new MyOptions ( args )

		// spark context
		val spark: SparkContext = options.getSparkContext

		// YOUR CONTRIBUTION HERE...
		

		//First, we load the file and strip off the header "Sym,Date,Open,High,Low,Close,Volume,Adjusted"
		val stocksFile = spark.textFile(options.inputPath).filter(line=> !line.contains("Sym"))
		
		//Now, transform it into tuples and cache it
		val stockTuples = stocksFile.map(line=>line.split(",")).cache
		
		//Produce RDD[(String, Int)] where the key is an unique symbol, and value is the count, then collect its array
		val symbolCounts = stockTuples.map(line=>(line(0),1)).reduceByKey(_+_)
		var symbolCount = symbolCounts.count.toInt
		  
		val closePriceMatrix : Array[Array[Double]] = new Array[Array[Double]](symbolCount)
		val volumeMatrix : Array[Array[Double]] = new Array[Array[Double]](symbolCount)
		var matrixIdx = 0

		//For each of the unique symbol, calculate the required statistics, and fill the matrices appropriately
		symbolCounts.take(symbolCount).foreach{ case(symbol, count) =>
		  result.append("[ Close Price statistics for " + symbol + " ]\n\n")
		  printStatsHeader
		  var symbolFilteredTuples = stockTuples.filter(tuple=>(tuple(0)==symbol)).cache
		  closePriceMatrix(matrixIdx) = calculateRequiredStats(symbolFilteredTuples, 5, count, result)
		  result.append("[ Volume statistics for " + symbol + " ]\n\n")
		  printStatsHeader
		  volumeMatrix(matrixIdx) = calculateRequiredStats(symbolFilteredTuples, 6, count, result)
		  matrixIdx += 1
		}
		
		result.append("[ Pearson product-moment correlation coefficients of Close Price among all stocks ]\n" )
		result.append(generatePearsonsCorrelationCoefficientMatrix(closePriceMatrix, symbolCount) + "\n\n")
		
		result.append("[ Pearson product-moment correlation coefficients of Volume among all stocks ]\n" )
		result.append(generatePearsonsCorrelationCoefficientMatrix(volumeMatrix, symbolCount) + "\n\n")
		
		// save any results ... example follows
		HdfsUtils.putHdfsFileText ( options.outputPath + "/" + "test.txt",
			spark.hadoopConfiguration, result.toString, true )

		// stop Spark
		spark.stop()
	}
	
	
	// Homework Helper functions
	
	var result = new StringBuilder

	def mode(xs: RDD[Double]) : Double = xs.map(value=>(value, 1)).reduceByKey(_+_).map(pair=>pair.swap).sortByKey(false).first._2
	
	def median(xs: Array[Double]): Double = xs(xs.size / 2)
	  
	def quartiles(xs: Array[Double]): (Double, Double, Double) =
	  (xs(xs.size / 4), median(xs), xs(xs.size / 4 * 3))
	  
	def iqr(xs: Array[Double]): Double = quartiles(xs) match {
		case (lowerQuartile, _, upperQuartile) => upperQuartile - lowerQuartile
	}
	
	def movingAverage(values: Array[Double], period: Int): List[Double] = {
	   val first = (values take period).sum / period
	   val subtract = values map (_ / period)
	   val add = subtract drop period
	   val addAndSubtract = add zip subtract map Function.tupled(_ - _)
	   val res = (addAndSubtract.foldLeft(first :: List.fill(period - 1)(0.0)) { 
	     (acc, add) => (add + acc.head) :: acc 
	   }).reverse
	   res
	}
	
	def calculateRequiredStats(tuples: RDD[Array[String]], tupleIndex: Int, count: Int, result: StringBuilder ) : Array[Double] = {
	      //tuple(1) is date in YYYY-MM-DD, we use as key and sort it by key
		  var sortedTupleByDate = tuples.map(tuple=>(tuple(1),tuple(tupleIndex).toDouble)).sortByKey(true).map(dateTuplePair=>dateTuplePair._2)
		  var sortedTupleByDateArray = sortedTupleByDate.collect
		  var descStatTuple = new DescriptiveStatistics(sortedTupleByDateArray)
		  var rddFuncTuple = new DoubleRDDFunctions(sortedTupleByDate)
		  result.append(descStatTuple.getMin +"\t"+ descStatTuple.getMax +"\t"+ count 
		      +"\t"+ rddFuncTuple.mean +"\t"+ mode(sortedTupleByDate) +"\t"+ median(sortedTupleByDateArray) 
		      +"\t"+ rddFuncTuple.variance +"\t"+ rddFuncTuple.stdev +"\t"+ descStatTuple.getKurtosis()
		      +"\t"+ iqr(sortedTupleByDateArray) + "\n\n"
		  )
		  result.append("histogram<20-bucket> in [lower-bound, upper-bound), frequency  \n")
		  try {
			  val hist = rddFuncTuple.histogram(20)
			  for (i <- 0 until hist._2.length) {
			    result.append(hist._1(i) + "\t" + hist._1(i+1) + "\t" + hist._2(i) + "\n")
			  }
			  result.append("\n")
		  } catch {
	         case ex: Exception =>{
	            result.append("*** Please submit a bug report to Spark 0.9.0 on DoubleRDDFunctions.histogram(bucketCount: Int), or use a different bucketCount value (e.g. for fb it needs at least 22 buckets) - " + ex + "\n\n")
	         }
	      }
		  result.append("3 day moving average\n")
		  movingAverage(sortedTupleByDateArray, 3).foreach(movingAve=>result.append(movingAve+"\t"))
		  result.append("\n\n\n")
		  sortedTupleByDateArray
	}
	
	def generatePearsonsCorrelationCoefficientMatrix( twoDimArray: Array[Array[Double]], numSymbols: Int ) : String = {
	  val matrix = new StringBuilder
	  val pc = new PearsonsCorrelation
	  matrix.append("{\n")
	  for (i <- 0 until numSymbols) {
	    matrix.append("  {")
	    for (j <- 0 until numSymbols ) {
	    	matrix.append(pc.correlation(twoDimArray(i), twoDimArray(j)) + ", ")
	    }
	    matrix.append("},\n")
	  }
	  matrix.append("}")
	  matrix.toString
	}
	
	def printStatsHeader(): Unit = { result.append("minimum\tmaximum\tcount\tmean\tmode\tmedian\tvariance\tstandard deviation\tkurtosis\tIQR\n") }

}
