package com.zx.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class HdfsDemo02 {
	public static void main(String[] args) {

		Configuration conf = new Configuration();//获得配置文件信息
		
		URI uri = URI.create("hdfs://master:9000/");//文件系统uri
		try {
			
			DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(uri, conf);
			Path file = new Path("/data/cite75_99.txt");
			
			FileStatus fstatus = fs.getFileStatus(file);//文件信息对象
			
			BlockLocation[] bls = fs.getFileBlockLocations(fstatus, 0, fstatus.getLen());//文件详情对象
			
			System.out.println("文件被分为: " + bls.length + "块");
			
			for (BlockLocation block : bls) {
				
				String[] hosts = block.getHosts();
				
				String[] cachehosts = block.getCachedHosts();
				
				long dataSize = block.getLength();
				System.out.println("dataSize = " + dataSize);
				
				long offset = block.getOffset();
				
				System.out.println("offset = " + offset);
				
				System.out.println("cachehosts = " + cachehosts.length);
				
				System.out.println(block);
			}
		
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
}
