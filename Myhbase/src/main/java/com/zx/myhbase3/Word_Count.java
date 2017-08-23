package com.zx.myhbase3;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Word_Count {

	public static class Mymap extends TableMapper<Text, IntWritable> {

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {

			List<Cell> cells = value.listCells();
			String v;
			for (Cell cell : cells) {
				v = Bytes.toString(CellUtil.cloneValue(cell));
				context.write(new Text(v), new IntWritable(1));
			}

		}

	}

	public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int num = 0;
			for (IntWritable a : values) {
				num += a.get();
			}

			context.write(key, new IntWritable(num));
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "word_count");
		job.setJarByClass(Word_Count.class);
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob("ns1:t2", scan, Mymap.class, Text.class, IntWritable.class, job);
		job.setReducerClass(MyReduce.class);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/out" + System.currentTimeMillis()));
	
		job.waitForCompletion(true);
	}

}
