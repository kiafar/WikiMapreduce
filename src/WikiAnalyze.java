import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WikiAnalyze extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
    	// Tool runner parses command line options automatically
        int res = ToolRunner.run(new Configuration(), new WikiAnalyze(), args);
        System.exit(res); // 0=success, 1=Some failed jobs
    }

	@Override
	public int run(String[] args) throws Exception {
        /* run() From interface: org.apache.hadoop.util.Tool */
        System.out.println("In Driver now! --------------------------------");
        // check required CLI arguments (i.e. I/O paths)
        if (args.length != 2) {
            System.err.println("Usage: WikiAnalyze [-D Option] <in_dir> <out_dir>");
            System.err.println("Output directories: out_dir_sites, out_dir_langs");
            System.err.println("Options:\norder.desc==(true||false)\nrecords.num==(Integer)");
            System.exit(-1); }
        
        // Extract arguments from command line
        // Temporary path to store combined results, will be deleted at the end of method
        Path tmp_path = new Path(args[1]+"_tmp");
        Path tmp_path_lang = new Path(args[1]+"_tmp_lang");
        // "order.desc" CLI option sets whether sort is descending. "-D order.desc=true" or "false"
        boolean asc = getConf().get("order.desc", "true").toLowerCase()=="true"?true:false;
        // "records.num" CLI option limits number of records for output
        int recNum = getConf().getInt("records.num", -1);
        System.out.println("records.num: " + String.valueOf(recNum));
        // ControlledJob for each job, will be chained later by a JobControl
        // countStep combines input files, unifies hits for page in a bigger time lapse
        ControlledJob countStep = new ControlledJob(
        		getSitesCountJob(new Path(args[0]), tmp_path), null);
        ControlledJob langStep = new ControlledJob(
        		getLangFormatJob(tmp_path, tmp_path_lang), null);
        // all the jobs need output from countStep. So langStep can start immediately after it.
        langStep.addDependingJob(countStep);
        ControlledJob sitesSortStep = new ControlledJob(
        		getSitesSortJob(tmp_path, new Path(args[1]+"_sites"), asc, recNum), null);
        sitesSortStep.addDependingJob(countStep);
        ControlledJob langSortStep = new ControlledJob(
        		getLangSortJob(tmp_path_lang, new Path(args[1]+"_langs"), asc, recNum), null);
        // langStep is the direct dependency => countStep is an indirect dependency
        langSortStep.addDependingJob(langStep);

        // JobControl encapsulates the defined jobs
        JobControl ctrl = new JobControl("WikiAnalyze");
        ctrl.addJob(countStep);
        ctrl.addJob(langStep);
        ctrl.addJob(sitesSortStep);
        ctrl.addJob(langSortStep);
        // making a thread and run the JobControl inside
        Thread workflowThread = new Thread(ctrl, "Workflow-Thread");
        workflowThread.setDaemon(true);
        workflowThread.start();
        
        // Run the JobControl, wait till all jobs are finished
        while (!ctrl.allFinished())
        	  Thread.sleep(500);
        // Check for failed jobs, prompt user if there are any
        int res = 0; // success
    	if (ctrl.getFailedJobList().size() > 0 ){
    		res = 1; // Some jobs failed
    		System.out.println(ctrl.getFailedJobList().size() + " jobs failed!"); 
    		for ( ControlledJob job : ctrl.getFailedJobList())
    			System.out.println(job.getJobName() + " failed");
    	} else
    		System.out.println("Success!! Workflow completed [" +
    				ctrl.getSuccessfulJobList().size() + "] jobs");
    	
    	// remove intermediate folder. Filesystem needs namenode, gets it from Configuration()
    	FileSystem fs = FileSystem.get(new Configuration());
    	// second boolean argument enables recursive deletion
    	fs.delete(tmp_path, true);
    	fs.delete(tmp_path_lang, true);
    	
    	return res;
	}
	
	/* First job, combines input files, its output is input for other jobs */
	public Job getSitesCountJob(Path ip, Path op)throws IOException {
		// Get a job instance and set it up
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WikiAnalyze.class); 
        job.setJobName("WikiSitesCount");
        
        job.setMapperClass(wikisites.SiteMapper.class);
        // combiner and reducer for this type of job are identical
        job.setCombinerClass(wikisites.SiteReducer.class);
        job.setReducerClass(wikisites.SiteReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Set I/O format. Sequence is a binary, size friendly format
        // Other jobs need to set their input format to Sequence
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // Set recursive scanning of directory before adding input directory
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, ip);
        SequenceFileOutputFormat.setOutputPath(job, op);
        
        return job;
	}
	
	public Job getLangFormatJob(Path ip, Path op)throws IOException {
		// Get a new instance of job, set it up
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WikiAnalyze.class); 
        job.setJobName("WikiLangFormat");
        
        // Setup mapreduce classes
        job.setMapperClass(wikilangs.LangMapper.class);
        job.setCombinerClass(wikilangs.LangReducer.class);
        // combiner and reducer identical
        job.setCombinerClass(wikilangs.LangReducer.class);
        job.setReducerClass(wikilangs.LangReducer.class);
        
        // Setup key value types for I/O
        job.setMapOutputKeyClass(Text.class);
        // IntsWritable format: TEXT INT INT
        job.setMapOutputValueClass(wikilangs.IntsWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(wikilangs.IntsWritable.class);
        
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        
        // Add I/O paths
        SequenceFileInputFormat.addInputPath(job, ip);
        SequenceFileOutputFormat.setOutputPath(job, op);
        
        return job;
	}
	
	/* Gets a single sqeuence input and sorts results based on integer value */
	public Job getSitesSortJob(Path ip, Path op, boolean desc, int recNum)throws IOException {
		// Create new configurations, set optional values on configuration object
		Configuration conf = new Configuration();
		conf.setInt("records.num", recNum);
		// Create job with conf and set it up
        Job job = Job.getInstance(conf);
        job.setJarByClass(WikiAnalyze.class);
        job.setJobName("WikiSitesSort");
        
        // mapper writes key/value swapped, value/key
        job.setMapperClass(sitessort.SortMapper.class);
        // The custom comparator class sorts the output of the mapper based on
        // keys (page count) before sending to reducer
        job.setSortComparatorClass(getSitesComparatorClass(desc));
        job.setReducerClass(sitessort.SortReducer.class);
        // Ordered results must go to one reducer.
        job.setNumReduceTasks(1);
        
        // Set key/value input output, order of input and output are reversed
        // at the end, it happens in reducer after sort done on mapper output
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Set the input class as the input file is a sequence file
        job.setInputFormatClass(SequenceFileInputFormat.class);
        
        SequenceFileInputFormat.addInputPath(job, ip);
        FileOutputFormat.setOutputPath(job, op);
        
        return job;
	}
	
	/* Sorts the combined language data */ 
	public Job getLangSortJob(Path ip, Path op, boolean desc, int recNum)throws IOException {
		
		Configuration conf = new Configuration();
		// Set limit for number of records for output
		conf.setInt("records.num", recNum);
		
        Job job = Job.getInstance(conf);
        job.setJarByClass(WikiAnalyze.class);
        job.setJobName("WikiLangSort");
        
        // mapper writes key/value swapped, value/key
        job.setMapperClass(langsort.SortMapper.class);
        // The custom comparator class sorts the output of the mapper based on
        // keys (page count) before sending to reducer
        job.setSortComparatorClass(getSitesComparatorClass(desc));
        job.setReducerClass(langsort.SortReducer.class);
        // Ordered results must go to one reducer. To make sure user cannot override that
        job.setNumReduceTasks(1);
        
        // Set key/value input output, order of input and output are reversed
        // at the end, it happens in reducer after sort done on mapper output
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(langsort.MedWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(langsort.AvgWritable.class);
        
        // Set the input class, imposed by the output class of first job
        job.setInputFormatClass(SequenceFileInputFormat.class);
        
        SequenceFileInputFormat.addInputPath(job, ip);
        FileOutputFormat.setOutputPath(job, op);
        
        return job;
	}
	/* Returns proper ascending or descending Comparator based on passed argument */
	public Class<? extends WritableComparator> getSitesComparatorClass(boolean desc) {
		if (desc)
			return sitessort.IntDescComparator.class;
		return sitessort.IntAscComparator.class;
	}

}
