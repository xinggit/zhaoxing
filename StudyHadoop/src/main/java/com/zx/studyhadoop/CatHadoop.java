package com.zx.studyhadoop;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

public class CatHadoop {
	public static void main(String[] args) {
		URL url = null;
		
		try {
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
			url = new URL("hdfs://master:9000/data/1.sh");
			URLConnection con = url.openConnection();
			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"));
			String line = null;
			while((line = in.readLine()) != null) {
				System.out.println(line);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
