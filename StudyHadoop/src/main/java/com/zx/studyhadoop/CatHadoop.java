package com.zx.studyhadoop;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class CatHadoop {
	public static void main(String[] args) {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		URL url = null;
//		try {
//			url = new URL("hdfs://master:9000/data/1.sh");
//			URLConnection con = url.openConnection();
//			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"));
//			String line = null;
//			while((line = in.readLine()) != null) {
//				System.out.println(line);
//			}
//			
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		try {
			url = new URL("hdfs://master:9000/data/1.sh");
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
