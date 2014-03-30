/* Copyright (c) 2013. Oracle and/or its affiliates. All rights reserved. */

package oracle.recruitment.util

import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import scala.collection.JavaConversions._

/**
 * Please see OptionsDriver for examples and documentation 
 * on how to use this trait.
 */

trait Options {

	val parser = new CmdLineParser(Options.this)

	def initialize(args: Array[String]) = {
		try {
			parser.parseArgument(args.toList)
		} catch {
			case e: CmdLineException => {
				parser.printUsage(System.err)
				throw e
			}
		}
	}

	def printUsage() {
		parser.printUsage(System.err)
	}
}
