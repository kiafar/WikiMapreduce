package wikisites;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Reducer for sitesCountJob */
public class SiteReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private static int counter;
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
    	// Add the page hit for the same page, from different input files
    	counter = 0;
        for (IntWritable value:values)
        	counter += value.get();
        context.write(key, new IntWritable(counter));
    }

}