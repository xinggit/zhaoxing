package com.zx.example8;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce extends Reducer<DataHot, Text, Text, Text> {

	@Override
	protected void reduce(DataHot key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		context.write(new Text(key.getData() + ""), new Text(key.getHot() + ""));

	}

}
