package com.zx.hadoop2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zx.hadoop.JobRun;

public class TwoSortDemo {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TwoSortDemo");
		
		job.setJar("target/Hadoop-0.0.1-SNAPSHOT.jar");
		
		job.setJarByClass(JobRun.class);
		
		job.setReducerClass(SogouReduce.class);
		job.setMapperClass(SogouMap.class);
		
		//自定义输入数据格式
		job.setInputFormatClass(MyInputFormat.class);
		
		job.setMapOutputKeyClass(KeyPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/test02/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/out" + System.currentTimeMillis()));
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
