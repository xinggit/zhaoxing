package com.zx.example4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LinkReduce extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

		List<Text> grandparents = new ArrayList<Text>();
		List<Text> grandchild = new ArrayList<Text>();

		for (Text text : value) {
			String line = text.toString();
			if (line.startsWith("+")) {
				grandparents.add(new Text(line.substring(1)));
			} else {
				grandchild.add(new Text(line.substring(1)));
			}

		}

		for (Text text1 : grandchild) {

			for (Text text2 : grandparents) {
				context.write(text1, text2);
			}

		}

	}

}
