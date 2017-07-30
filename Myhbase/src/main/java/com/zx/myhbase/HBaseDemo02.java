package com.zx.myhbase;

import java.io.IOException;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDemo02 {

	private static Connection con;
	private static Admin admin;

	static {

		try {
			Configuration conf = HBaseConfiguration.create();
//			System.out.println(conf.get("nameaaa"));
			con = ConnectionFactory.createConnection(conf, Executors.newCachedThreadPool());
			admin = con.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void put() throws Exception {
		
		Table table = con.getTable(TableName.valueOf(Bytes.toBytes("zx:t1")));
		Put put = new Put(Bytes.toBytes("row1"));
		put.addColumn("chengji".getBytes(), "name".getBytes(), Bytes.toBytes("liujia"));
		table.put(put);
		System.out.println("插入成功");
		admin.close();
		con.close();
	}
	
	public static void get() throws Exception {
		
		Table table = con.getTable(TableName.valueOf("t1"));
		Get get = new Get("row1".getBytes());
		table.get(get);
		System.out.println("插入成功");
		admin.close();
		con.close();
	}
	
	public static void drop() throws Exception {
		admin.deleteTable(TableName.valueOf(Bytes.toBytes("t1")));
	}

	public static void create() throws Exception {

		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("zx:t1"));
		HColumnDescriptor family = new HColumnDescriptor("chengji");
		desc.addFamily(family);
		admin.createTable(desc);
		System.out.println("创建成功");
		admin.close();
		con.close();
	}

	public static void main(String[] args) throws Exception {
		put();
	}
}
