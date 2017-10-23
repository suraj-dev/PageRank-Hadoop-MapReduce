package assignment3.PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 * This class extends the default Mapper class provided by hadoop and overrides the map method.
 * It computes the pageRank distribution from each node to its adjacent nodes and in case of dangling nodes, updates the
 * global delta value.
 */
public class PageRankMapper extends Mapper<Object, Text, Text, NodeDetails> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Double pageRank = 0.0;
		Configuration conf = context.getConfiguration();
		//retrieve number of nodes from configuration
		long noOfNodes = conf.getLong("noOfNodes", 1);
		
		//initial page rank value
		Double initialPageRank = 1.0 / noOfNodes;
		String[] input = value.toString().split("\t");
		if (input.length >= 2) {
			//node name
			String page = input[0].trim();
			//adjacency list
			String links = input[1].trim();

			NodeDetails graph = new NodeDetails();
			graph.setPageRank(new DoubleWritable(0.0));
			graph.setIsNode(new BooleanWritable(true));
			graph.setLinks(new Text(links));
			//emit node and its graph structure
			context.write(new Text(page), graph);
			
			//after first iteration, pageRank value from previous iteration is available as part of the value to the mapper
			if (input.length == 3) {
				try {
					pageRank = Double.parseDouble(input[2]);
				} catch (Exception e) {
					pageRank = 0.0;
				}
			}
			
			//if this is the first iteration, set pageRank to initial pageRank value
			if (pageRank == 0.0) {
				pageRank = initialPageRank;
			}
			
			//check for dangling node
			if (links.equals("[]")) {
				//retrieve global delta counter
				Double delta = context.getCounter(Driver.counters.RUNNING_DELTA).getValue() / Math.pow(10, 10);
				delta += pageRank;
				long delta_long_val = (long) (delta * Math.pow(10, 10));
				// Update global counter with the latest delta value.
				context.getCounter(Driver.counters.RUNNING_DELTA).setValue((delta_long_val));
			} else {

				if (links.length() > 2) {
					//retrieve adjacency list
					String[] adjList = links.substring(1, links.length() - 1).split(",");
					int adjList_length = adjList.length;
					// page rank contribution of node to each of its neighbors
					Double pageRank_contribution = pageRank / adjList_length;

					for (String node : adjList) {
						NodeDetails pageRankNode = new NodeDetails();
						pageRankNode.setPageRank(new DoubleWritable(pageRank_contribution));
						pageRankNode.setIsNode(new BooleanWritable(false));
						// Emit node and its pageRank object without the graph structure
						context.write(new Text(node), pageRankNode);

					}
				}
			}

		}
	}
}
