package com.zx.example8;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;

public class MyMap extends Mapper<LongWritable, Text, DataHot, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String k = line.substring(0, 8);
		String v = line.substring(8);
		Log.info("======line = " + line);
		Log.info("======key = " + k + ",value = " + v);

		context.write(new DataHot(Integer.parseInt(v), Integer.parseInt(k)), value);

	}

}
