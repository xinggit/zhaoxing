package com.zx.example;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanReduce extends Reducer<DataType, NullWritable, Text, Text> {

	@Override
	protected void reduce(DataType key, Iterable<NullWritable> value, Context context)
			throws IOException, InterruptedException {

		context.write(new Text(key.getDate()), new Text(key.getData()));

	}

}
