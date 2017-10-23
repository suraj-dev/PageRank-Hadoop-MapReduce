package assignment3.PageRank;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKReducer extends Reducer<NullWritable, Text, Text, Text> {
	
	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		//holds the page rank and name of the node
		TreeMap<Double, String> nodeRanks = new TreeMap<Double, String>();
		
		for(Text value : values) {
			//retrieve node nad page rank pair
			String[] nodeRankPair = value.toString().split(" ");
			//retrieve name of the node
			String node = nodeRankPair[0];
			Double pageRank;
			try {
				//retrieve page rank
				pageRank = Double.parseDouble(nodeRankPair[1]);
			}
			catch(Exception e) {
				pageRank = 0.0;
			}
			
			//insert into tree map
			nodeRanks.put(pageRank, node);
			
			//check if Tree map exceeds 100 records as we only want top 100.
			if(nodeRanks.size() > 100)
				nodeRanks.remove(nodeRanks.firstKey());
		}
		
		//iterate through the keys of the tree map and emit (node, pageRank) pairs.
		for (Double pageRank : nodeRanks.descendingMap().keySet()) {
			context.write(new Text(nodeRanks.get(pageRank)), new Text(pageRank.toString()));
		}
	}
}
