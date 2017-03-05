package langsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/* A custom writable to hold langCode and hits (TEXT INT) */ 
public class MedWritable implements Writable {
	private Text lang = new Text();
	private IntWritable hits = new IntWritable();
	public MedWritable() {}
	public MedWritable(Text lang, IntWritable hits) {
		this.lang = lang;
		this.hits = hits;
	}
	// getters for lang and hits, makes them read only, set by constructor */
	public Text getLang() {return lang;}
	public IntWritable getHits() {return hits;}
	
	/* read data from input, called automatically by mapper/reducer */
	@Override
	public void readFields(DataInput in) throws IOException {
		lang.readFields(in);
		hits.readFields(in);
	}

	/* Writes data to the output */
	@Override
	public void write(DataOutput out) throws IOException {
		lang.write(out);
		hits.write(out);
	}
	
	/* needed to convert data to readable strings on write */
	public String toString() {
		return lang.toString()+"\t"+hits.toString();
	}
}
