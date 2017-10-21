package assignment3.PageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InitialComputationReducer extends Reducer<Text, NodeDetails, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<NodeDetails> value, Context context) throws IOException, InterruptedException {
		
		context.getCounter(Driver.counters.NO_OF_NODES).increment(1);
		Text links = new Text();
		for(NodeDetails node : value) {
			//check if node is an actual node with a graph structure
			if(node.getIsNode().get() == true && node.getIsDanglingNode().get() == false) {
				links = node.getLinks();
				context.write(key, links);
			}
			//check for dangling nodes
			else if (node.getIsNode().get() == true && node.getIsDanglingNode().get() == true) {
				context.write(key, new Text("[]"));
			}
		}
	}
}
