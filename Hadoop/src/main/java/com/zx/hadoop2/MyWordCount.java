package com.zx.hadoop2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//1.对map输出的key排序
//2.合并相同的key，value组成集合，如：hadoop ---> key :hadoop value: (1,1,1) 

public class MyWordCount {

	public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Context context)
				throws IOException, InterruptedException {
			
			StringBuilder sb = new StringBuilder();
			
			for (IntWritable i : value) {
				sb.append(i + ",");
			}
			
			System.out.println("***********>key = " + key + "，" + sb.toString());
			
		}

	}

	// 默认将数据文件中数据拆分中一行一行
	// key 是每一行数据在文件中首字母出现的位置
	// value 每一行数据
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] word = value.toString().trim().split("\\s+");

			for (String string : word) {
				// 上下文: 全局共享对象所存放的map 简称:上下文
				context.write(new Text(string), new IntWritable(1));
			}

		}

	}

	public static void main(String[] args) {

		Configuration conf = new Configuration(); // hadoop配置文件

		try {

			Job job = Job.getInstance(conf, "MyWordCount");// mapreduce 作业对象

			job.setMapperClass(MyMapper.class);
			job.setJarByClass(MyWordCount.class);// 设置作业处理类
			job.setReducerClass(MyReduce.class);
			
			// 注意: 当输入输出数据不一致时，需要指定输入输出数据
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			
			FileInputFormat.setInputPaths(job, new Path("hdfs://master:9000/data/1.sh"));// 设置处理数据文件的位置
			FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/data_out"));// 设置处理后文件的存放位置
			System.exit(job.waitForCompletion(true) ? 0 : 1);// 开始执行作业

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
