package com.zx.hadoop3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce3 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {
		
		for (IntWritable intWritable : value) {
			context.write(key, intWritable);
		}
		
	}

}
