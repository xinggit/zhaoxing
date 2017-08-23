package com.zx.myhbase2;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class MyReducer extends TableReducer<Text, ImmutableBytesWritable, ImmutableBytesWritable> {

	@Override
	protected void reduce(Text key, Iterable<ImmutableBytesWritable> values, Context context)
			throws IOException, InterruptedException {

		for (ImmutableBytesWritable text : values) {
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("index"), text.get());
			context.write(new ImmutableBytesWritable(key.getBytes()), put);
		}
	}

}
