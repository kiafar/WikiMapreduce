package langsort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* mapper for langSortJob, reverses key values because comparator can sort records with (int) keys */
public class SortMapper extends Mapper<Text, wikilangs.IntsWritable, IntWritable, MedWritable> {
	public void map(Text key, wikilangs.IntsWritable value, Context context)
    		throws IOException, InterruptedException {
		// en:[456, 1] => 456:[en, 1] so that it could be sorted by key(int)
		context.write(new IntWritable(value.getCount()), new MedWritable(key, new IntWritable(value.getHits())));
   }
}