package com.zx.example6;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConnectMap1 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();

		String[] kv = line.split("\t");

		if (kv.length == 2) {

			if (kv[0].matches("[0-9]")) {
				context.write(new Text(kv[0]), new Text(kv[1]));
			} else if (kv[1].matches("[0-9]")) {
				context.write(new Text(kv[1]), new Text("+" + kv[0]));
			}
		}
	}

}
