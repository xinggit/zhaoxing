package com.zx.hadoop5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoSortDemo {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TwoSortDemo");

		job.setJarByClass(TwoSortDemo.class);

//		job.setReducerClass(SogouReduce.class);
//		job.setMapperClass(SogouMap.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path("hdfs://master:9000/test02/"), TextInputFormat.class, SogouMap.class);

		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/out" + System.currentTimeMillis()));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
