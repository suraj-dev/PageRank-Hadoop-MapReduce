package assignment3.PageRank;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * This class holds the configuration for the job responsible for computing the Top K records
 */
public class TopKJobConfig {
	public static void getTopKRecords(String input, String output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException  {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TopK");
		job.setJarByClass(Driver.class);
		// Set Mapper Class
		job.setMapperClass(TopKMapper.class);
		// Set Reducer class
		job.setReducerClass(TopKReducer.class);
 		// Set Map output key class type
		job.setMapOutputKeyClass(NullWritable.class);
		// Set Map output value class type.
		job.setMapOutputValueClass(Text.class);
		// Set Reducer output class type
		job.setOutputKeyClass(Text.class);
		// Set Reduer output value class type
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(input));	
		// Set output path
		FileOutputFormat.setOutputPath(job,new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
