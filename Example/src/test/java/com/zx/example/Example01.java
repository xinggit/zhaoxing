package com.zx.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Example01 {

	private static Configuration conf = new Configuration();

	public static void main(String[] args) throws Exception {

		conf.set("fs.defaultFS", "hdfs://master:9000/");

		Job job = Job.getInstance(conf, "Example01");
		job.setJarByClass(Example01.class);

		job.setMapperClass(Example01Map.class);
		job.setReducerClass(Example01Reducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path("/example/test01"));
		FileOutputFormat.setOutputPath(job, new Path("/out" + System.currentTimeMillis()));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static class Example01Map extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			int num = Integer.parseInt(value.toString());

			context.write(new IntWritable(num), NullWritable.get());

		}

	}

	public static class Example01Reducer extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {

		private int sum = 1;

		@Override
		protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {

			context.write(new IntWritable(sum), key);
			sum++;

		}

	}

}
