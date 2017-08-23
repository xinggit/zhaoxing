package com.zx.studyhadoop;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class Demo01 {
	public static void main(String[] args) {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		URL url = null;
		
		try {
			url = new URL("hdfs://master:9000/out/part-r-00000");//218.196.14.101
			URLConnection con = url.openConnection();
//			IOUtils.copyBytes(con.getInputStream(), System.out, 2048, true);
			IOUtils.copyBytes(con.getInputStream(), System.out, 1024, true);
			
			
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
