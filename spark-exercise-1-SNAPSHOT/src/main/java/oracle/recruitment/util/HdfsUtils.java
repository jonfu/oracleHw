/* $Header$ */

/* Copyright (c) 2014. Oracle and/or its affiliates. All rights reserved. */

package oracle.recruitment.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URI;

/**
 * Useful filesystem utilities.
 *
 * @author Kevin L. Markey
 * @version $Revision: 18495 $
 * @date $Date: 2014-02-20 18:30:59 -0700 (Thu, 20 Feb 2014) $
 * @created 11/19/13, 11:01 PM
 * @since JDK1.6
 */

public class HdfsUtils implements Serializable
{
	public final static String COPYRIGHT = "Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.";

	public static int putHdfsFileText ( String path, Configuration conf, String s, boolean overwrite )
	{
		PrintWriter writer = null;
		FileSystem fs = null;
		int count = 0;
		try {
			// Do not use System.getProperty("user.name") because doAs(sparkUser)
			// does not override the value of this property.
			// Instead, call get() without a user, and it will determine the
			// current authority from the URL.
			fs = FileSystem.get(URI.create(path), conf);
			Path p = new Path(path);
			FSDataOutputStream out = fs.create(p,overwrite);
			writer = new PrintWriter ( new BufferedWriter ( new OutputStreamWriter ( out, "UTF-8"  ) ) );
			writer.println ( s );
			count++;
		}
		catch ( Exception e ) {
			e.printStackTrace();
			return 0;
		}
		finally {
			try {
				writer.close();
				fs.close();
			}
			catch ( IOException e ) {}
		}
		return count;
	}
}
