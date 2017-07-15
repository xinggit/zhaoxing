package com.zx.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 此例证明，2.x之后支持追加文件
 * @author zx
 *
 */
public class HdfsDemo04 {
	public static void main(String[] args) {

		Configuration conf = new Configuration();
		try {
			URI uri = new URI("hdfs://master:9000/data/1.sh");
			
			FileSystem fs = FileSystem.get(uri, conf);
			FSDataOutputStream out = fs.append(new Path("/data/1.sh"));
			out.write("aaa".getBytes());
			out.flush();
			out.hflush();
			out.close();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
