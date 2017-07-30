package com.zx.myhbase;

import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

public class HBaseDemo01 {

	public static void main(String[] args) throws Exception {
		//配置
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "slave01,slave02,slave03");
		//通过连接工厂获取连接
		Connection con = ConnectionFactory.createConnection(conf, Executors.newCachedThreadPool());
		//工具类bytes
		Table table = con.getTable(TableName.valueOf("t1"));
		Put put = new Put("row1".getBytes());
		put.addColumn("c1".getBytes(), "name".getBytes(), "赵星".getBytes());
		table.put(put);
		System.out.println("插入成功");
		table.close();
		con.close();
		
	}
	
}
