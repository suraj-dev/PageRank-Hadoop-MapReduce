package assignment3.PageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InitialComputationReducer extends Reducer<Text, NodeDetails, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<NodeDetails> value, Context context) throws IOException, InterruptedException {
		//get global counter of No of nodes from configuration
		context.getCounter(Driver.counters.NO_OF_NODES).increment(1);
		Text links = new Text();
		for(NodeDetails node : value) {
			//check if node is an actual node with a graph structure
			if(node.getIsNode().get() == true && node.getIsDanglingNode().get() == false) {
				links = node.getLinks();
				//emit node and its graph structure
				context.write(key, links);
			}
			//check for dangling nodes
			else if (node.getIsNode().get() == true && node.getIsDanglingNode().get() == true) {
				//emit dangling node
				context.write(key, new Text("[]"));
			}
		}
	}
}
