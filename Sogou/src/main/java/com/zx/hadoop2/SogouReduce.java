package com.zx.hadoop2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SogouReduce extends Reducer<KeyPair, IntWritable, IntWritable, IntWritable> {

	@Override
	protected void reduce(KeyPair key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {

		for (IntWritable intWritable : value) {
			context.write(new IntWritable(key.getFrist()), new IntWritable(key.getSecond()));
		}

	}

}
