package com.zx.myhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


public class HBase_MR {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job = Job.getInstance(conf, "job");
		
		job.setJarByClass(HBase_MR.class);
		
		TableMapReduceUtil.initTableMapperJob(TableName.valueOf("blog"), new Scan(), MyMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob("res", MyReducer.class, job);
//		FileOutputFormat.setOutputPath(job, new Path("/out" + System.currentTimeMillis()));
		job.waitForCompletion(true);
	}
	
}
