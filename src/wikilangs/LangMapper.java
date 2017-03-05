package wikilangs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* mapper for langFormatJob */
public class LangMapper extends Mapper<Text, IntWritable, Text, IntsWritable> {
    private static String[] words;
	public void map(Text key, IntWritable value, Context context)
    		throws IOException, InterruptedException {
		// en.sitename 456 => en : [456, 1]
		words = key.toString().split("\\.");
		// Write the part of key before dot, and the composite value
		context.write(new Text(words[0]), new IntsWritable(value.get(), 1));
   }
}