package com.zx.example5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConnectReduce extends Reducer<IntWritable, Text, Text, Text> {

	@Override
	protected void reduce(IntWritable key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {

		List<Text> ks = new ArrayList<>();
		Text v = null;

		for (Text text : value) {
			String name = text.toString();
			if (name.startsWith("+")) {
				ks.add(new Text(name.substring(1)));
			} else {
				v = new Text(text);
			}
		}
		if (v != null) {
			for (Text text : ks) {
				context.write(text, v);
			}
		}
	}

}
