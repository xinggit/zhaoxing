package com.zx.hadoop3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Map3 extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {

	@Override
	protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
	
		context.write(key, value);
		
	}

}
