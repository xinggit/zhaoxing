package com.zx.studyhadoop2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce02 extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

		Set<Text> set = new HashSet<Text>();
		for (Text text : value) {
			set.add(text);
		}

		for (Iterator<Text> iterator = set.iterator(); iterator.hasNext();) {
			Text text1 = iterator.next();
			for (Iterator<Text> iterator2 = set.iterator(); iterator2.hasNext();) {
				Text text2 = iterator2.next();
				if (!text1.equals(text2)) {
					context.write(text1, text2);
				}
			}
		}

	}

}
