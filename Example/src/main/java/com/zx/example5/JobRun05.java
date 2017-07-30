package com.zx.example5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRun05 {

	private static Configuration conf = new Configuration();

	public static void main(String[] args) throws Exception {

		conf.set("fs.defaultFS", "hdfs://master:9000");

		Job job = Job.getInstance(conf, "JobRun05");

		job.setJarByClass(JobRun05.class);

		job.setReducerClass(ConnectReduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path("/example/test04/data02"), TextInputFormat.class, ConnectMap2.class);
		MultipleInputs.addInputPath(job, new Path("/example/test04/data01"), TextInputFormat.class, ConnectMap1.class);
		FileOutputFormat.setOutputPath(job, new Path("/out" + System.currentTimeMillis()));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
