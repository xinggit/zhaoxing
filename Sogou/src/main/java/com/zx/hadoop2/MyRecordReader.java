package com.zx.hadoop2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class MyRecordReader extends RecordReader<IntWritable, IntWritable> {

	private LineRecordReader line;
	private IntWritable key;
	private IntWritable value;
	
	public MyRecordReader() {
		line = new LineRecordReader();
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		line.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(key == null) {
			key = new IntWritable();
		}
		
		if(value == null) {
			value = new IntWritable();
		}
		
		if(line.nextKeyValue()) {
			String[] kv = line.getCurrentValue().toString().split("\\s+");
			if(kv.length != 0) {
				key.set(Integer.parseInt(kv[3]));
				value.set(Integer.parseInt(kv[4]));
				return true;
			}
		}
		
		return false;
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public IntWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return line.getProgress();
	}

	@Override
	public void close() throws IOException {
		line.close();
	}

}
