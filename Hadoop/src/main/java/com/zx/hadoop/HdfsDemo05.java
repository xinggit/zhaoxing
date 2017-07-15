package com.zx.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsDemo05 {
	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		
		URI uri = URI.create("hdfs://master:9000/");
		
		try {
			FileSystem fs = FileSystem.get(uri, conf);
			
			Path file = new Path("/data/1.sh");
			FSDataInputStream in = fs.open(file, 4096);
			IOUtils.copyBytes(in, System.out, conf);
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
