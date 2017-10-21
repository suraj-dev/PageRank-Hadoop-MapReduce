package assignment3.PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PageRankMapper extends Mapper<Object, Text, Text, NodeDetails> {
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Double pageRank = 0.0;
		Configuration conf = context.getConfiguration();
		long noOfNodes = conf.getLong("noOfNodes", 1);
		
		Double initialPageRank = 1.0 / noOfNodes; 
		String[] input = value.toString().split("\t");
		if(input.length >= 2) {
			String page = input[0].trim();
			String links = input[1].trim();
			
			NodeDetails graph = new NodeDetails();
			graph.setPageRank(new DoubleWritable(0.0));
			graph.setIsNode(new BooleanWritable(true));
			graph.setLinks(new Text(links));
			
			context.write(new Text(page), graph);
			
			if (input.length == 3) {
				try {
					pageRank = Double.parseDouble(input[2]);
				} catch (Exception e) {
					pageRank = 0.0;
				}
			}
			
			if(pageRank == 0.0) {
				pageRank = initialPageRank;
			}
			
			if (links.length() > 2) {
				String[] adjList = links.substring(1, links.length() - 1).split(",");
				int adjList_length = adjList.length;
				// page rank contribution to be sent to outlinks in adjacency list
				Double pageRank_contribution = pageRank / adjList_length;

				for (String node : adjList) {
					// Initialize a new node with its page rank contribution and 
					// do not send the graph structure for these nodes.
					NodeDetails pageRankNode = new NodeDetails();
					pageRankNode.setPageRank(new DoubleWritable(pageRank_contribution));
					pageRankNode.setIsNode(new BooleanWritable(false));
						// Emit page and its node object
					context.write(new Text(page), pageRankNode);
					
				}
			}
			
			if(links.equals("[]")) {
				Double delta = context.getCounter(Driver.counters.RUNNING_DELTA).getValue() / Math.pow(10, 10);
		    	delta += pageRank;
				long delta_long_val = (long) (delta * Math.pow(10, 10));
				// Update global counter with the latest delta value.
				context.getCounter(Driver.counters.RUNNING_DELTA).setValue((delta_long_val));
			}
			
		}
	}
}
