package langsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Reducer for langSortJob, incoming records are already sorted by a comparator */
public class SortReducer extends Reducer<IntWritable, MedWritable, Text, AvgWritable> {
	// Stores a number for output records
	private static int recordLimit;
	
	/* setup is run before actual reduction job, used here to get custom properties */
    protected void setup(Context context) throws IOException, InterruptedException {
    	// this conf comes from Toolrunner, containing command line options
        Configuration conf = context.getConfiguration();
        // user can set this from command line
        recordLimit = conf.getInt("records.num", -1);
    }
    
    public void reduce(IntWritable count, Iterable<MedWritable> values, Context context)
            throws IOException, InterruptedException {
    	// default value of -1 has no effect. 0 or more records are effective.
    	if (recordLimit==0)
    		return;
    	else
    		recordLimit--;
    	
    	// Output: langCode: [count, hits, average_hit/page] (TEXT INT INT DOUBLE)
        for (MedWritable med : values)
        	context.write(med.getLang(), new AvgWritable(
        			count.get(),
        			med.getHits().get(),
        			med.getHits().get()/(double)count.get()));
    }

}