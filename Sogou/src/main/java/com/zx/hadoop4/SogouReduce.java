package com.zx.hadoop4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SogouReduce extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

		List<Text> list = new ArrayList<>();

		boolean flag = false;
		for (Text text : value) {
			list.add(text);
			if (text.toString().contains("房价")) {
				flag = true;
			}
		}

		if (flag) {
			for (Text text : list) {
				context.write(key, text);
			}
		}

	}

}
