package com.zx.example6;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConnectReduce extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {

		Set<Text> ks = new HashSet<>();
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
