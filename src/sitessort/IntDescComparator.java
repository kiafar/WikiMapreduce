package sitessort;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.IntWritable;

/* A custom descending comparator */ 
public class IntDescComparator extends WritableComparator {

    public IntDescComparator() {
    	// Set IntWritble as key type in WritableComparator
        super(IntWritable.class, true);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
    		byte[] b2, int s2, int l2) {
    	// each {b,s,l} group present an Integer, by byteBuffer
    	// -1 changes the order of comparison 
        return -1 * super.compare(b1, s1, l1, b2, s2, l2);
    }
}