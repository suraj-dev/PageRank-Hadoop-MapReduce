package assignment3.PageRank;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitialComputationMapper extends Mapper<Object, Text, Text, NodeDetails>{
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] input = value.toString().split("\t");
		if(input.length >= 2) {
			String page = input[0].trim();
			String links = input[1].trim();
			String adjacencyListWithoutDuplicates = "";
			HashSet<String> nodes = new HashSet<String>();
			
			if(links.equals("[]")) {
				adjacencyListWithoutDuplicates = links;
				NodeDetails danglingNode = new NodeDetails(new DoubleWritable(0.0), new Text(adjacencyListWithoutDuplicates), 
						new BooleanWritable(true), new BooleanWritable(true));
				context.write(new Text(page), danglingNode);
			}
			
			else {
				String[] adj_list = links.substring(1, links.length() - 1).split(",");
				adjacencyListWithoutDuplicates = "[";
				for(String Node : adj_list) {
					if(!nodes.contains(Node)) {
						nodes.add(Node);
						adjacencyListWithoutDuplicates += Node + ",";
					}
				}
				adjacencyListWithoutDuplicates = adjacencyListWithoutDuplicates.substring(0, adjacencyListWithoutDuplicates.length() - 1);
				adjacencyListWithoutDuplicates += "]";
				NodeDetails newNode = new NodeDetails(new DoubleWritable(0.0), new Text(adjacencyListWithoutDuplicates.trim()), 
						new BooleanWritable(true), new BooleanWritable(false));
				
				Iterator<String> hs = nodes.iterator();
				while(hs.hasNext()) {
					NodeDetails pageRankNode = new NodeDetails();
					pageRankNode.setPageRank(new DoubleWritable(0.0));
					pageRankNode.setIsNode(new BooleanWritable(false));
					pageRankNode.setIsDanglingNode(new BooleanWritable(false));
					context.write(new Text(hs.next()), pageRankNode);
				}
				
			}
			
		}
	}
}
