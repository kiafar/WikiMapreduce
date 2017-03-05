package wikisites;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* Mapper for sitesCountJob */
public class SiteMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	// Static to reduce load of instantiations
    private static String[] words;
    
	public void map(LongWritable key, Text value, Context context)
    		throws IOException, InterruptedException {
        //Split by white spaces
		words = value.toString().split("\\s+");
		// Input: "en Throat_cancer 2 48265" output: "en.Throat_cancer 2"
		// To filter out non-wikipedia pages. Wikipedia pages dont have ".something" in their URL
		if (!words[0].contains("."))
			context.write(new Text(words[0]+"."+words[1]),
					new IntWritable(Integer.parseInt(words[2])));
   }
}