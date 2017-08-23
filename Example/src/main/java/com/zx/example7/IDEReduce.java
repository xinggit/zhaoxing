package com.zx.example7;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.mortbay.log.Log;

public class IDEReduce extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder("{");
		
		for (Text text : values) {
			Log.info("============text = " +text.toString());
			String[] kv = text.toString().split("-");
			String name = kv[0];
			String sum = kv[1];
			sb.append("[" + name + "," + sum + "],");
		}
		String value = sb.toString().substring(0, sb.lastIndexOf(","));
		
		context.write(key, new Text(value + "}"));
	}

}
