package com.zx.example;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;

public class CleanMap extends Mapper<LongWritable, Text, DataType, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] data = line.split("\\s");

		if (data.length == 2) {
			Log.info("============data[0]" + data[0] + ",data[1]" + data[1]);
			context.write(new DataType(data[0], data[1]), NullWritable.get());
			
		}

	}

}
