package langsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/* A custom writable for language data, format: INT INT DOUBLE */
public class AvgWritable implements Writable {
	
	private int count = 0;
	private int hits = 0;
	private double avg = 0;
	public AvgWritable(int count, int hits, double avg) {
		this.count = count;
		this.hits = hits;
		this.avg = avg;
	}
	/* read data from input, called automatically by mapper/reducer */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		count = in.readInt();
		hits = in.readInt();
		avg = in.readDouble();
	}

	/* Writes data to the output */
	@Override
	public void write(DataOutput out) throws IOException {
		// To handle context.write(ValsWritable);
		out.writeInt(count);
		out.writeInt(hits);
		out.writeDouble(avg);
	}
	
	/* needed to convert data to readable strings on write */
	public String toString() {
		return Integer.toString(count)+"\t"+Integer.toString(hits)+"\t"+Double.toString(avg);
	}
}
