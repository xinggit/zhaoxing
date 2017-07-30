package com.zx.example2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	private int num = 1;
	
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {
		
		for (IntWritable intWritable : value) {
			context.write(new IntWritable(num), key);
			num++;
			
		}
		
	}

}
