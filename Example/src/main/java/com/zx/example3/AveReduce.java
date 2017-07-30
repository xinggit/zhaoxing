package com.zx.example3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AveReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		int size = 0;

		for (IntWritable intWritable : value) {
			sum += intWritable.get();
			size++;
		}

		context.write(key, new IntWritable(sum / size));

	}

}
