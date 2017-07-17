package com.zx.hadoop5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JobRun {
	public static SequenceFile.Reader reader;

	public static void main(String[] args) throws Exception {
		String path = "hdfs://master:9000/data/test/data";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "reader");

		// 设置map之前的，即从hdfs上获得，还未输入到map的数据格式
		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setJarByClass(JobRun.class);
		reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(path)));

		FileInputFormat.addInputPath(job, new Path(path));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/out" + System.currentTimeMillis()));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
