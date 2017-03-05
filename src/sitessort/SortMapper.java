package sitessort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* mapper for siteSortJob, reverses key values because comparator can sort records with (int) keys */
public class SortMapper extends Mapper<Text, IntWritable, IntWritable, Text> {
	public void map(Text key, IntWritable value, Context context)
    		throws IOException, InterruptedException {
		// Change the order to value/key
		context.write(value, key);
   }
}