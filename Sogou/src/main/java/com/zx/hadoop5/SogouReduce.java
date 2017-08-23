package com.zx.hadoop5;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SogouReduce extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {

		for (Iterator<Text> i = value.iterator(); i.hasNext();) {

			if (i.next().toString().contains("房价")) {
				for (Iterator<Text> j = value.iterator(); j.hasNext();) {
					context.write(key, j.next());
				}
			}

		}

	}

}
