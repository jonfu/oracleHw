package oracle.recruitment.exercise

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.kohsuke.args4j.Option
import oracle.recruitment.util.{HdfsUtils, Options}


// These will be useful
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.DoubleRDDFunctions
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics
import org.apache.commons.math.stat.correlation.PearsonsCorrelation
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
		
		
		var result = new StringBuilder
		//var result2 = new StringBuilder
		def printStatHeader(): Unit = { result.append("minimum\tmaximum\tcount\tmean\tmode\tmedian\tvariance\tstandard deviation\tkurtosis\tIQR\n") }
		

	

		//First, we load the file and strip off the header "Sym,Date,Open,High,Low,Close,Volume,Adjusted"
		var stocksFile = spark.textFile(options.inputPath).filter(line=> !line.contains("Sym"))
		
		//Now, transform it into tuples and cache it
		var stockTuples = stocksFile.map(line=>line.split(",")).cache
		
		//Produce RDD[(String, Int)] where the key is an unique symbol, and value is the count
		var symbolCounts = stockTuples.map(line=>(line(0),1)).reduceByKey(_+_)
		
		println("symbolCounts is " + symbolCounts + ", count is " + symbolCounts.count)
		
		symbolCounts.take(symbolCounts.count.toInt).foreach{ case(symbol, count) =>
		  result.append("[ Closed Price statistics for " + symbol + " ]\n\n")
		  printStatHeader
		  var symbolFilteredTuples = stockTuples.filter(tuple=>(tuple(0)==symbol)).cache
		  calculateRequiredStats(symbolFilteredTuples, 5, count, result)
		  
		  result.append("[ Volume statistics for " + symbol + " ]\n\n")
		  printStatHeader
		  calculateRequiredStats(symbolFilteredTuples, 6, count, result)

		}	
		  		
		  		
		  		
		  		
		  		
		  		
		  		
		  		
		  		
		  		
		  		
		  		
		  		
		  		

		
		result.append("File count is ")
		result.append(stocksFile.count)



		// save any results ... example follows
		HdfsUtils.putHdfsFileText ( options.outputPath + "/" + "test.txt",
			spark.hadoopConfiguration, result.toString, true )

		// stop Spark
		spark.stop()
	}
	
	
	// Homework Helper functions
	
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
	
	def descriptiveStatistics(xs: Array[Double]) : DescriptiveStatistics = {
		val sd = new DescriptiveStatistics
		for (i<-0 until xs.length)
		  sd.addValue(xs(i))
		sd
	}
	
	def calculateRequiredStats(tuples: RDD[Array[String]], tupleIndex: Int, count: Int, result: StringBuilder ) : Unit = {
		  var sortedTupleByDate = tuples.map(tuple=>(tuple(1),tuple(tupleIndex).toDouble)).sortByKey(true).map(dateTuplePair=>dateTuplePair._2)
		  var sortedTupleByDateArray = sortedTupleByDate.collect
		  var descStatTuple = descriptiveStatistics(sortedTupleByDateArray)
		  var rddFuncTuple = new DoubleRDDFunctions(sortedTupleByDate)
		  result.append(descStatTuple.getMin +"\t"+ descStatTuple.getMax +"\t"+ count 
		      +"\t"+ rddFuncTuple.mean +"\t"+ mode(sortedTupleByDate) +"\t"+ median(sortedTupleByDateArray) 
		      +"\t"+ rddFuncTuple.variance +"\t"+ rddFuncTuple.stdev +"\t"+ descStatTuple.getKurtosis()
		      +"\t"+ iqr(sortedTupleByDateArray) + "\n\n"
		  )
		  result.append("histogram of frequency (20 buckets)\n" + rddFuncTuple.histogram(20) + "\n\n");
		  result.append("3 day moving average\n" + movingAverage(sortedTupleByDateArray, 3) + "\n\n\n\n");
	}
	
	
}
