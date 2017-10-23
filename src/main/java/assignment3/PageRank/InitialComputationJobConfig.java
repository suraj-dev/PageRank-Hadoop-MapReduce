package assignment3.PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * This class handles the configuration of the job for Initial Computation that computes the total number of nodes and also
 * identifies dangling nodes
 */
public class InitialComputationJobConfig {
	public static long compute(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "initialComputationJob");		
		job.setJarByClass(Driver.class);
		// Set the Mapper Class here
		job.setMapperClass(InitialComputationMapper.class);
		// Set the Reducer Class here
		job.setReducerClass(InitialComputationReducer.class);
		// Set the Mapper Output key class here
		job.setMapOutputKeyClass(Text.class);
		// Set the Mapper Output value class here
		job.setMapOutputValueClass(NodeDetails.class);
		// Set the Reducer output key class
		job.setOutputKeyClass(Text.class);
		// Set the Reducer output value class
		job.setOutputValueClass(Text.class);
		// Read Input path from the arguments	
		FileInputFormat.setInputPaths(job, new Path(input));	
		// Set output path
		FileOutputFormat.setOutputPath(job,new Path(output));
		job.waitForCompletion(true);		
		// Get the number of nodes from the global counter after the job is completed.
		long noOfNodes = job.getCounters().findCounter(Driver.counters.NO_OF_NODES).getValue();		
		// Return the number of nodes to the driver program.
		return noOfNodes;
	}
}
