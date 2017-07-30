package com.zx.example8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zx.example7.JobRun07;

public class JobRun08 {
	private static Configuration conf = new Configuration();
	
	public static void main(String[] args) throws Exception {
		
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job = Job.getInstance(conf, "JobRun07");
		
		job.setJarByClass(JobRun07.class);

		job.setNumReduceTasks(8);
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		
		
		job.setMapOutputKeyClass(DataHot.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("/example/test08/*"));
		FileOutputFormat.setOutputPath(job, new Path("/out" + System.currentTimeMillis()));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
