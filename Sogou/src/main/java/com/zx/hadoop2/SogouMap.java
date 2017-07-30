package com.zx.hadoop2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;

public class SogouMap extends Mapper<IntWritable, IntWritable, KeyPair, IntWritable> {

	@Override
	protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {

		Log.info("=========================key = " + key + ",value = " + value);
		context.write(new KeyPair(key.get(), value.get()), value);
		
	}

}
