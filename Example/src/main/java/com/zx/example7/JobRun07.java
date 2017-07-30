package com.zx.example7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JobRun07 {

	private static Configuration conf = new Configuration();
	
	public static void main(String[] args) throws Exception {
		
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job = Job.getInstance(conf, "JobRun07");
		
		job.setJarByClass(JobRun07.class);

		job.setCombinerClass(MyCombiner.class);
		
		job.setReducerClass(IDEReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path("/example/test07/*"), TextInputFormat.class, IDFMap.class);
		FileOutputFormat.setOutputPath(job, new Path("/out" + System.currentTimeMillis()));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
