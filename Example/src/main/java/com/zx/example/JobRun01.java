package com.zx.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * 数据去重"主要是为了掌握和利用并行化思想来对数据进行有意义的筛选。
 * 统计大数据集上的数据种类个数、
 * 从网站日志中计算访问地等这些看似庞杂的任务都会涉及数据去重。
 * 下面就进入这个实例的MapReduce程序设计。
 * @author zx
 *
 */
public class JobRun01 {
	
	private static Configuration conf = new Configuration();
	
	public static void main(String[] args) throws Exception {
		
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job = Job.getInstance(conf, "job01");
		job.setJarByClass(JobRun01.class);
		
		job.setMapperClass(CleanMap.class);
		job.setReducerClass(CleanReduce.class);
		
		job.setMapOutputKeyClass(DataType.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path("/example/*.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/out" + System.currentTimeMillis()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
