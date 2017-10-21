package assignment3.PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, NodeDetails, Text, Text> {
	
	public long noOfNodes;
	public Double delta;
	public Configuration conf;
	
	@Override
	public void setup(Context context) {
		conf = context.getConfiguration();
		delta = conf.getDouble("runningDelta", 0.0);
		noOfNodes = conf.getLong("noOfNodes", 1);
	}
	
	@Override
	public void reduce(Text key, Iterable<NodeDetails> value, Context context) throws IOException, InterruptedException {
		Double alpha = 0.15;
		Double pageRankSum = 0.0;
		Double finalPageRank = 0.0;
		String links = "";
		
		for(NodeDetails node : value) {
			if(node.getIsNode().get() == true) {
				links = node.getLinks().toString();
			}
			else {
				pageRankSum += node.getPageRank().get();
			}
		}
		
		finalPageRank = (alpha / noOfNodes) + ((1 - alpha) * (pageRankSum + (delta / noOfNodes)));
		
		context.write(key, new Text(links + "\t" + finalPageRank));
	}
}
