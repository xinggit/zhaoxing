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

public class Example02 {

	private static Configuration conf = new Configuration();

	public static void main(String[] args) throws Exception {

		conf.set("fs.defaultFS", "hdfs://master:9000/");

		Job job = Job.getInstance(conf, "Example02");
		job.setJarByClass(Example02.class);

		job.setMapperClass(Example02Map.class);
		job.setReducerClass(Example02Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path("/example/test02"));
		FileOutputFormat.setOutputPath(job, new Path("/out" + System.currentTimeMillis()));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static class Example02Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] kv = value.toString().split("\\s+");

			context.write(new Text(kv[0]), new IntWritable(Integer.parseInt(kv[1])));

		}

	}

	public static class Example02Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int index = 0;
			int sum = 0;
			for (IntWritable i : values) {
				index++;
				sum += i.get();
			}

			context.write(key, new IntWritable(sum / index));

		}

	}

}
