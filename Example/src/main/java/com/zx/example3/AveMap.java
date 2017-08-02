package com.zx.example3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AveMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] kv = line.split("\\s+");

		if (kv.length == 2) {
			
			context.write(new Text(kv[0]), new IntWritable(Integer.parseInt(kv[1])));
			
		}

	}

}
