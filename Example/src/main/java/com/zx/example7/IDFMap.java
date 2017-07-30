package com.zx.example7;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IDFMap extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// https://stackoverflow.com/questions/11130145/hadoop-multipleinputs-fails-with-classcastexception
		InputSplit split = context.getInputSplit();
		Class<? extends InputSplit> splitClass = split.getClass();

		FileSplit fileSplit = null;
		if (splitClass.equals(FileSplit.class)) {
			fileSplit = (FileSplit) split;
		} else if (splitClass.getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {

			try {
				Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
				getInputSplitMethod.setAccessible(true);
				fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
			} catch (Exception e) {
				throw new IOException(e);
			}

			String line = value.toString();
			String[] words = line.split("\\s+");

			for (String string : words) {
				context.write(new Text(string), new Text(fileSplit.getPath().getName()));
			}
		}
	}

}
