package assignment3.PageRank;

import org.apache.hadoop.io.IntWritable;

public class Driver {
	
	public enum counters {
		NO_OF_NODES,
		RUNNING_DELTA,
		PREVIOUS_DELTA
	}
	
	public static void main(String[] args) throws Exception {
		String job1_outputPath = Parser.parseXML(args);
		String job2_outputPath = job1_outputPath + "initialComputationOutput";
		
		long noOfNodes = InitialComputationJobConfig.compute(job1_outputPath, job2_outputPath);
		
		Double delta = 0.0;
		String previousResultsPath = job2_outputPath;
		
		for(int i=1 ; i <= 10 ; i++) {
			delta = PageRankJobConfig.computePageRank(previousResultsPath, previousResultsPath + "_" + i, noOfNodes, delta);
			previousResultsPath = previousResultsPath + "_" + i;
		}
	}
}
