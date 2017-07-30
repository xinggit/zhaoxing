package com.zx.hadoop3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zx.hadoop2.MyInputFormat;

public class JobRun3 {
	
	private static Configuration conf = new Configuration();
	
	public static void main(String[] args) throws Exception {
		
		Job job = Job.getInstance(conf, "TwoSortDemo");
		
		job.setJar("target/Hadoop-0.0.1-SNAPSHOT.jar");
		
		job.setJarByClass(JobRun3.class);
		
		job.setReducerClass(Reduce3.class);
		job.setMapperClass(Map3.class);
		
		//自定义输入数据格式
		job.setInputFormatClass(MyInputFormat.class);
		
		job.setSortComparatorClass(Mysort2.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/test02/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/out" + System.currentTimeMillis()));
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
