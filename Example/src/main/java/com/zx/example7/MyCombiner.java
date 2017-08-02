package com.zx.example7;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyCombiner extends Reducer<Text, Text, Text, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
		Text v = new Text();
		for (Text text : values) {
			sum += 1;
			v.set(text.toString() + "-" + sum);
		}
		
		context.write(key, v);
		
	}
	
}
