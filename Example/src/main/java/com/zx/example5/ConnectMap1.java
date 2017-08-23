package com.zx.example5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConnectMap1 extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
		String line = value.toString();
		
		String[] kv = line.split("\t");
		
		if(kv.length == 2) {
			context.write(new IntWritable(Integer.parseInt(kv[1])), new Text("+" + kv[0]));
			
		}
		
	}

}
