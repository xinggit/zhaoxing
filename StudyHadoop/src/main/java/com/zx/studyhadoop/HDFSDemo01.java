package com.zx.studyhadoop;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HDFSDemo01 {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		
		try {
			URL url = new URL("hdfs://master:9000");
			FileSystem.get(url.toURI(),conf,"hadoop");//
			
			
			
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
