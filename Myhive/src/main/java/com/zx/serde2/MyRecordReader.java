package com.zx.serde2;

import java.io.IOException;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import jline.internal.Log;

public class MyRecordReader implements RecordReader<LongWritable, Text> {

	private LineRecordReader line;
	private LongWritable lineKey = null;
	private Text lineValue = null;

	public MyRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) {
		reporter.setStatus(genericSplit.toString());
		String delimiter = job.get("textinputformat.record.delimiter");
		byte[] recordDelimiterBytes = null;
		if (null != delimiter) {
			recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
		}
		try {
			line = new LineRecordReader(job, (FileSplit) genericSplit, recordDelimiterBytes);
		} catch (IOException e) {
			e.printStackTrace();
		}
		lineKey = line.createKey();
		lineValue = line.createValue();
	}

	@Override
	public boolean next(LongWritable key, Text value) throws IOException {

		if (line.next(lineKey, lineValue)) {

			String v = lineValue.toString();
			if (v.startsWith("<DOC>") && v.endsWith("</DOC>")) {

				value.set(v.substring(5, v.lastIndexOf("/") - 1));
			}
			key.set(key.get() + 1);
			Log.info("input = " + value.toString());
			return true;

		}
		return false;
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable(0);
	}

	@Override
	public Text createValue() {
		return new Text("");
	}

	@Override
	public long getPos() throws IOException {
		return line.getPos();
	}

	@Override
	public void close() throws IOException {
		line.close();
	}

	@Override
	public float getProgress() throws IOException {
		return line.getProgress();
	}
}
