package wikilangs;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Reducer for langFormatJob */
public class LangReducer extends Reducer<Text, IntsWritable, Text, IntsWritable> {

	public void reduce(Text key, Iterable<IntsWritable> values, Context context)
            throws IOException, InterruptedException {
		// create output value: hits, count (INT INT) 
		IntsWritable compositeValue = new IntsWritable();
		// ValsWritable.merge(newVar) increments hits and count by that of newVar
    	for (IntsWritable vals: values)
    		compositeValue.merge(vals);
    	// Output: URL: [ hits, count ] (TEXT INT INT)
        context.write(key, compositeValue);
    }

}