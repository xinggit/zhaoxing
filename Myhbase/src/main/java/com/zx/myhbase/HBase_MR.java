package com.zx.myhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class HBase_MR {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "job");
		
		job.setJarByClass(HBase_MR.class);
		
		job.setMapperClass(MyMapper.class);
		
		
	}
	
}
