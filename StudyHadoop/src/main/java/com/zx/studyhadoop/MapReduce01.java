package com.zx.studyhadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//对应关系
//LongWritable   ---->    long
//Text           ---->    String
//IntWritable    ---->    int
public class MapReduce01 extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		super.map(key, value, context);
		
		
		
	}
}
