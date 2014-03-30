package oracle.recruitment.exercise

import org.apache.spark.SparkContext
import org.kohsuke.args4j.Option
import oracle.recruitment.util.{HdfsUtils, Options}


// These will be useful
import org.apache.spark.rdd.RDD
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



		// save any results ... example follows
		HdfsUtils.putHdfsFileText ( options.outputPath + "/" + "test.txt",
			spark.hadoopConfiguration, "Hello, World!", true )

		// stop Spark
		spark.stop()
	}
}
