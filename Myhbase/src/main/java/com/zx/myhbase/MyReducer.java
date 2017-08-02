package com.zx.myhbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		
		StringBuilder value = new StringBuilder();
		
		for (Text text : values) {
			value.append(text.toString() + ",");
		}
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.addColumn(Bytes.toBytes("person"), Bytes.toBytes("nickname"), value.toString().substring(0, value.length() - 1).getBytes());
		context.write(new ImmutableBytesWritable(key.getBytes()),put);
		
	}
	
}
