package com.zx.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsDemo06 {
	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		
		URI uri = URI.create("hdfs://master:9000/");
		
		try {
			FileSystem fs = FileSystem.get(uri, conf);
			
			Path file = new Path("/out/part-r-00000");
			FSDataInputStream in = fs.open(file, 4096);
			int len = -1;
			byte[] buf = new byte[520];
			
			
			IOUtils.skipFully(in, 1024);
			IOUtils.readFully(in, buf, 0, buf.length);
			System.out.println(new String(buf));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
