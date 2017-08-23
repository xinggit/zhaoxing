package com.zx.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.log.Log;

public class Example03 {

	private static Configuration conf = new Configuration();

	public static void main(String[] args) throws Exception {

		conf.set("fs.defaultFS", "hdfs://master:9000/");

		Job job = Job.getInstance(conf, "Example03");
		job.setJarByClass(Example03.class);

		job.setMapperClass(Example03Map.class);
		job.setReducerClass(Example03Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("/example/test03"));
		FileOutputFormat.setOutputPath(job, new Path("/out" + System.currentTimeMillis()));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static class Example03Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] kv = value.toString().split("\\s+");

			context.write(new Text(kv[0]), new Text("parent=" + kv[1]));

			context.write(new Text(kv[1]), new Text("child=" + kv[0]));

		}

	}

	public static class Example03Reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			List<String> child = new ArrayList<>();
			List<String> parent = new ArrayList<>();

			for (Text text : values) {
				Log.info("text = " + text.toString());
				String v = text.toString();
				if (v.startsWith("child"))
					child.add(v.split("=")[1]);
				if (v.startsWith("parent"))
					parent.add(v.split("=")[1]);
			}

			Log.info("child = " + child.size());
			Log.info("parent = " + parent.size());
			for (String ch : child) {

				for (String pa : parent) {
					context.write(new Text(ch), new Text(pa));
				}

			}

		}

	}

}
