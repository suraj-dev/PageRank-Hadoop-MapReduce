package assignment3.PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, NodeDetails, Text, Text> {
	
	public long noOfNodes;
	public Double delta;
	public Configuration conf;
	
	//this method retrieves the delta and no of nodes from the configuration object
	@Override
	public void setup(Context context) {
		conf = context.getConfiguration();
		delta = conf.getDouble("PREVIOUS_DELTA", 0.0);
		noOfNodes = conf.getLong("noOfNodes", 1);
	}
	
	@Override
	public void reduce(Text key, Iterable<NodeDetails> value, Context context) throws IOException, InterruptedException {
		Double alpha = 0.15;
		//holds the running page rank sum
		Double pageRankSum = 0.0;
		//holds the page rank value after computation
		Double finalPageRank = 0.0;
		String links = "";
		
		for(NodeDetails node : value) {
			//check if node is a Node object and retrieve graph structure
			if(node.getIsNode().get() == true) {
				links = node.getLinks().toString();
			}
			//get page rank value from page rank object and update the running page rank variable
			else {
				pageRankSum += node.getPageRank().get();
			}
		}
		
		//compute the page rank
		finalPageRank = (alpha / noOfNodes) + ((1 - alpha) * (pageRankSum + (delta / noOfNodes)));
		
		//emit node and its adjacency list with page rank value
		context.write(key, new Text(links + "\t" + finalPageRank));
	}
}
