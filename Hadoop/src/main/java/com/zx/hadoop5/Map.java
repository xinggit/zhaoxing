package com.zx.hadoop5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		char[] zi = value.toString().toCharArray();

		for (char c : zi) {
			context.write(new Text(c + ""), new IntWritable(1));
		}

	}

}
