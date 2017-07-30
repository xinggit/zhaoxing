package com.zx.example2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job02 {

	private static Configuration conf = new Configuration();
	
	public static void main(String[] args) throws Exception {
	
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job = Job.getInstance(conf, "Job02");
		
		job.setJarByClass(Job02.class);
		
		job.setMapperClass(SortMap.class);
		job.setReducerClass(SortReduce.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		FileInputFormat.addInputPath(job, new Path("/example/test01/*.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/out" + System.currentTimeMillis()));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
