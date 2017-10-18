package assignment3.PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import assignment3.PageRank.Driver;

public class Parser {
	public static String parseXML(String[] args) {
		BasicConfigurator.configure();
	    Configuration conf = new Configuration();
	    String[] otherArgs = null;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: app <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    Job job = null;
		try {
			job = Job.getInstance(conf, "xmlParser");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    job.setJarByClass(Driver.class);
	    job.setMapperClass(Bz2WikiParser.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    for (int i = 0; i < otherArgs.length - 1; ++i) {
	      try {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    }
	    FileOutputFormat.setOutputPath(job,
	      new Path(otherArgs[otherArgs.length - 1]));
	    try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    //returning output path of files generated by the Bz2WikiParser
	    return otherArgs[otherArgs.length - 1];
	  }
}