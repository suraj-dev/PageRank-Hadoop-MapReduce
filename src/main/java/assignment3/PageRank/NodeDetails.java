package assignment3.PageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class NodeDetails implements WritableComparable<NodeDetails> {
	
	private DoubleWritable pageRank;
	public DoubleWritable getPageRank() {
		return pageRank;
	}

	public void setPageRank(DoubleWritable pageRank) {
		this.pageRank = pageRank;
	}

	public Text getLinks() {
		return links;
	}

	public void setLinks(Text links) {
		this.links = links;
	}

	public BooleanWritable getIsNode() {
		return isNode;
	}

	public void setIsNode(BooleanWritable isNode) {
		this.isNode = isNode;
	}

	public BooleanWritable getIsDanglingNode() {
		return isDanglingNode;
	}

	public void setIsDanglingNode(BooleanWritable isDanglingNode) {
		this.isDanglingNode = isDanglingNode;
	}

	private Text links;
	private BooleanWritable isNode;
	private BooleanWritable isDanglingNode;
	
	public NodeDetails() {
		this.pageRank = new DoubleWritable();
		this.links = new Text();
		this.isNode = new BooleanWritable();
		this.isDanglingNode = new BooleanWritable();
	}
	
	public NodeDetails(DoubleWritable rank, Text links, BooleanWritable isNode, BooleanWritable isDanglingNode) {
		this.pageRank = rank;
		this.links = links;
		this.isNode = isNode;
		this.isDanglingNode = isDanglingNode;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.pageRank.write(out);
		this.links.write(out);	
		this.isNode.write(out);
		this.isDanglingNode.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.pageRank.readFields(in);
		this.links.readFields(in);
		this.isNode.readFields(in);
		this.isDanglingNode.readFields(in);
	}

	@Override
	public int compareTo(NodeDetails o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
}
