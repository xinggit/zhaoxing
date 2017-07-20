package com.zx.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JobRun {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		//获取作业对象产生之前
//		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, ",");
		
		Job job = Job.getInstance(conf, "sogou");
		job.setJarByClass(JobRun.class);
		
		job.setReducerClass(Reduce.class);
		job.setMapperClass(Map.class);
		
		//自定义输入数据格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setCombinerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/test02/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/out/" + System.currentTimeMillis()));
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
