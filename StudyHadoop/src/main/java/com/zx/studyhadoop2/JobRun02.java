package com.zx.studyhadoop2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRun02 {
	
	public static void main(String[] args) {
		// 导入配置文件，即根据用户的配置来运行程序，
		// 比如:JobTracker是谁
		Configuration conf = new Configuration();
		conf.set("F:\\Java\\mygit\\zhaoxing\\StudyHadoop\\target", "StudyHadoop-0.0.1-SNAPSHOT.jar");
		conf.set("fs.default.name", "hdfs://master:9000/");
		conf.set("mapred.job", "master:9001");
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobRun02.class);// 告诉Job程序的入口
			job.setMapperClass(Map02.class);// 告诉job执行map的程序是谁
			job.setReducerClass(Reduce02.class);// 告诉job执行reduce的程序是谁
			job.setMapOutputKeyClass(Text.class);// 告诉job执行map输出的key类型
			job.setMapOutputValueClass(Text.class);// 告诉job执行map输出的value类型

			job.setNumReduceTasks(1);// 设置reduce任务的个数，默认是一

			//输入数据和输出数据
			FileInputFormat.addInputPath(job, new Path("/test/univercity.txt"));
			FileOutputFormat.setOutputPath(job, new Path("/out/qq"));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
