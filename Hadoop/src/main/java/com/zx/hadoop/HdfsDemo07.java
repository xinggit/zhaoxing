package com.zx.hadoop;

import java.io.ByteArrayOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import com.zx.util.HDFSUtil;

public class HdfsDemo07 {
	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		
		URI uri = URI.create("hdfs://master:9000/");
		
//		HDFSUtil.readFile("/data/1.sh", null);
		
		//在程序中使用
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		HDFSUtil.readFile("/data/1.sh", out);
		System.out.println(out.toString());
		
	}
}
