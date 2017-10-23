package assignment3.PageRank;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * This class provides extends the default Mapper class provided by hadoop and overrides the map method.
 * It removes duplicates within adjacency lists, identifies dangling nodes and finally emits the node and its graph structure.
 */
public class InitialComputationMapper extends Mapper<Object, Text, Text, NodeDetails>{
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] input = value.toString().split("\t");
		if(input.length >= 2) {
			//retrieve name of the node
			String page = input[0].trim();
			
			//retrieve adjacency list 
			String links = input[1].trim();
			String adjacencyListWithoutDuplicates = "";
			
			//hash set to store elements from adjacency list
			HashSet<String> nodes = new HashSet<String>();
			
			//check for dangling nodes
			if(links.equals("[]")) {
				adjacencyListWithoutDuplicates = links;
				//create a new node object with isNodeObject and isDanglingNode properties set to true
				NodeDetails danglingNode = new NodeDetails(new DoubleWritable(0.0), new Text(adjacencyListWithoutDuplicates.trim()), 
						new BooleanWritable(true), new BooleanWritable(true));
				
				//emit as dangling node
				context.write(new Text(page), danglingNode);
			}
			
			else {
				//retrieve adjacency list
				String[] adj_list = links.substring(1, links.length() - 1).trim().split(",");
				adjacencyListWithoutDuplicates = "[";
				//remove duplicate links
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
				//emit node and graph structure
				context.write(new Text(page), newNode);
				
				//iterate through the hashset and emit each link as a pageRankNode
				Iterator<String> hs = nodes.iterator();
				while(hs.hasNext()) {
					String nodeName = hs.next();
					if(!nodeName.equals(page)) {
						NodeDetails pageRankNode = new NodeDetails();
						pageRankNode.setPageRank(new DoubleWritable(0.0));
						pageRankNode.setIsNode(new BooleanWritable(false));
						pageRankNode.setIsDanglingNode(new BooleanWritable(false));
						context.write(new Text(nodeName), pageRankNode);
					}
				}
				
			}
			
		}
	}
}
