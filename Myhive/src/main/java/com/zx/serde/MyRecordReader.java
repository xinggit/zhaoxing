package com.zx.serde;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class MyRecordReader extends RecordReader<LongWritable, Text> {

	private LineRecordReader line;
	private LongWritable key;
	private Text value;
	
	public MyRecordReader() {
		line = new LineRecordReader();
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		line.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if(!line.nextKeyValue()) {
			return false;
		}
		
		if (key == null) {
			key = new LongWritable();
		}
		
		if (value == null) {
			value = new Text();
		}
		
		key.set(line.getCurrentKey().get());
		String v = line.getCurrentValue().toString();
		if(v.startsWith("<DOC>") && v.endsWith("</DOC>")) {
			
			value.set(v.substring(5, v.lastIndexOf("/") - 1));
		}
		
		return true;
		
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
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
