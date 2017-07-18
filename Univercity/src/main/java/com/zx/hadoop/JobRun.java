package com.zx.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRun {

	private static Configuration conf = new Configuration();

	public static void main(String[] args) throws Exception {
		
		Job job = Job.getInstance(conf, "Analysis_Univercity");
		
		job.setJarByClass(JobRun.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/test/univercity.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/test/out/" + System.currentTimeMillis()));
		System.exit(job.waitForCompletion(true) == true ? 0:1);
		
	}

}
