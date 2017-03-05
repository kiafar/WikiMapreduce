package wikilangs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/* A custom writable for page data, format: INT INT */
public class IntsWritable implements Writable {
	private int hits = 0;
	private int count = 0;
	public IntsWritable() {}
	public IntsWritable(int hits, int count) {
		this.hits = hits;
		this.count = count;
	}
	
	// getters for data, makes them read only, set by constructor */
	public int getHits() {return hits;}
	public int getCount() {return count;}
	
	/* read data from input, called automatically by mapper/reducer */
	@Override
	public void readFields(DataInput in) throws IOException {
		hits = in.readInt();
		count = in.readInt();
	}

	/* Writes data to the output */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(hits);
		out.writeInt(count);
	}

	/* Merges data of a class of the same type with current data */ 
	public void merge(IntsWritable next) {
		// Makes possible accumulating of data easily in iterations, less code in mapper/reducer.
		this.hits += next.hits;
		this.count += next.count;
	}
	
	public String toString() {
		// To get readable output.
		return Integer.toString(hits)+"\t"+Integer.toString(count);
	}
}
