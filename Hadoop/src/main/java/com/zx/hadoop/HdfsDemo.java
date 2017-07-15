package com.zx.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

public class HdfsDemo {
	public static void main(String[] args) {
		String path = "hdfs://master:9000/data/1.sh";
		URL url = null;
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());   //设置可以访问的网络协议
		URLConnection con = null;
		try {
			url = new URL(path);
			con = url.openConnection();
			
			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"));
			
			String line = null;
			while((line = in.readLine()) != null) {
				System.out.println(line);
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
				
		
	}
}
