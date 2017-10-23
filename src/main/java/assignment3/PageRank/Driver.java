package assignment3.PageRank;

import org.apache.hadoop.io.IntWritable;

/*
 * This class controls the execution of the complete program by chaining MapReduce jobs and feeding the output of
 * one job to the next
 */
public class Driver {
	
	//global counters for number of nodes, running and previous delta values
	public enum counters {
		NO_OF_NODES,
		RUNNING_DELTA,
		PREVIOUS_DELTA
	}
	
	//main job function
	public static void main(String[] args) throws Exception {
		String job1_outputPath = Parser.parseXML(args);
		String job2_outputPath = job1_outputPath + "initialComputationOutput";
		
		//retrieve number of jobs from InitialComputation job
		long noOfNodes = InitialComputationJobConfig.compute(job1_outputPath, job2_outputPath);
		
		Double delta = 0.0;
		//variable to track output paths of each iteration
		String previousResultsPath = job2_outputPath;
		
		for(int i=1 ; i <= 10 ; i++) {
			delta = PageRankJobConfig.computePageRank(previousResultsPath, previousResultsPath + "_" + i, noOfNodes, delta);
			previousResultsPath = previousResultsPath + "_" + i;
		}
		
		String pageRankOutput= previousResultsPath;
		String finalTopK = pageRankOutput + "_" + "TopK";
		// Run the Top-K Job. 
		TopKJobConfig.getTopKRecords(pageRankOutput, finalTopK);
	}
}
