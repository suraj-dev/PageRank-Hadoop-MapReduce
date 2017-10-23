package assignment3.PageRank;

import java.util.TreeMap;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * This class extends the default Mapper class provided by hadoop and overrides the map method.
 * It computes the top 100 records in each Map task and emits them
 */
public class TopKMapper extends Mapper<Object, Text, NullWritable, Text> {
	
	//holds a pageRank value and a node. Stores them in ascending order of pageRank values
	public TreeMap<Double, String> nodeRanks;
	
	@Override
	public void setup(Context context) {
		nodeRanks = new TreeMap<Double, String>();
	}
	
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] input = value.toString().split("\t");
		if(input.length >= 2) {
			//retrieve name of node
			String node = input[0].trim();
			//retrieve pageRank
			Double pageRank;
			try {
				pageRank = Double.parseDouble(input[2].trim());
			} catch (Exception e) {
				pageRank = 0.0;
			}
			String nodeRankPair = node + " " + pageRank; 
			nodeRanks.put(pageRank, nodeRankPair);
			
			//check if Tree map exceeds 100 records as we only want top 100.
			if(nodeRanks.size() > 100)
				nodeRanks.remove(nodeRanks.firstKey());
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		//emit all the records in the tree map in the format (constant value, node + pageRank)
		for (String nr : nodeRanks.values()) {
			context.write(NullWritable.get(), new Text(nr));
		}
	}
}
