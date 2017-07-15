package com.zx.hadoop;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class HdfsDemo03 {
	public static void main(String[] args) {

		// 增加:
		// 1.创建目录
		Configuration conf = new Configuration();
		URI uri = URI.create("hdfs://master:9000/");
		FSDataOutputStream out = null;
		FileInputStream in = null;
		try {

			FileSystem fs = FileSystem.get(uri, conf);
			Path dir = new Path("/data/1.sh");// 要创建的目录对象,一定要使用绝对路径

			if (fs.exists(dir)) {
				System.out.println("目录存在");
			} else {
				System.out.println("目录不存在,创建目录");
				fs.mkdirs(dir);
			}

			// 2.创建文件

			Path file = new Path("/data/2.sh");
			if (fs.createNewFile(file)) {
				System.out.println("创建文件成功");
			} else {
				System.out.println("创建文件失败");
			}
			out = fs.append(file, 4096, new Progressable() {

				@Override
				public void progress() {
					System.out.print(">>>");
				}
			});
			in = new FileInputStream("C:\\Users\\Administrator\\Desktop\\cite75_99.txt");
			IOUtils.copyBytes(in, out, 4096, true);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (in != null)
					in.close();
				if (out != null)
					out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
}
