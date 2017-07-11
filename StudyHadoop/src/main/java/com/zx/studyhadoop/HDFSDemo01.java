package com.zx.studyhadoop;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSDemo01 {
	public static void main(String[] args) {
		Configuration conf = new Configuration();//hadoop配置文件对象
		
		try {
			URI uri = new URI("hdfs://master:9000"); //hadoop分布式文件系统master所在的uri
			FileSystem fs = FileSystem.get(uri,conf,"hadoop");//根据配置和远程地址创建文件系统
			
			Path file = new Path("/data/1.sh");
			InputStream in = fs.open(file);
			IOUtils.copy(in, System.out, 1024);
			
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		
	}
}
