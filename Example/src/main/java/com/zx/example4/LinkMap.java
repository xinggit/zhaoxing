package com.zx.example4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LinkMap extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] kv = line.split("\\s+");
		if (kv.length == 2) {
			context.write(new Text(kv[0]), new Text("+" + kv[1]));
			context.write(new Text(kv[1]), new Text("-" + kv[0]));
		}

	}

}
