package com.zx.example5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;

public class ConnectMap2 extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
		String line = value.toString();
		
		String[] kv = line.split("\t");
		
		if(kv.length == 2) {
			Log.info("k = " + kv[0]);
			Log.info("v = " + kv[1]);
			context.write(new IntWritable(Integer.parseInt(kv[0])), new Text(kv[1]));
			
		}
		
	}

}
