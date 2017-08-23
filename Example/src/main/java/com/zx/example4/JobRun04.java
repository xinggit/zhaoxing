package com.zx.example4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRun04 {

	private static Configuration conf = new Configuration();
	
	public static void main(String[] args) throws Exception {
	
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job = Job.getInstance(conf, "JobRun04");
		
		job.setJarByClass(JobRun04.class);
		
		job.setMapperClass(LinkMap.class);
		job.setReducerClass(LinkReduce.class);
//		job.setGroupingComparatorClass(LinkGroup.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
			
		
		FileInputFormat.addInputPath(job, new Path("/example/test03/*"));
		FileOutputFormat.setOutputPath(job, new Path("/out" + System.currentTimeMillis()));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
