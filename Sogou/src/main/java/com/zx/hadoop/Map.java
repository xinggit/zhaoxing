package com.zx.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;

public class Map extends Mapper<Text, Text, Text, IntWritable>{
	
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		

		String[] vs = value.toString().split("\t");
		Log.info("vs[1] = " + vs[1]);
		context.write(new Text(vs[1]), new IntWritable(1));
		
	}
	
}
