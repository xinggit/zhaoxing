package com.zx.myhbase3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.mortbay.log.Log;

public class HbaseUtil {

	private static Configuration conf;
	private static Connection con;
	private static Admin admin;

	static {

		conf = HBaseConfiguration.create();
		try {
			con = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			Log.info("创建连接错误....................", e);
			System.exit(-1);
		}

		try {
			admin = con.getAdmin();
		} catch (IOException e) {
			Log.info("获取管理员失败....................", e);
			System.exit(-1);
		}

	}

	public static void main(String[] args) throws Exception {
		scan();
	}
	
	@Test
	public static void scan() throws Exception {
		
		HTable table = (HTable) con.getTable(TableName.valueOf("student"));
		
		Scan scan = new Scan();
	
		Filter filter = new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("90")));
		scan.setFilter(filter);
		
		ResultScanner result = table.getScanner(scan);
		for (Result result2 : result) {
			List<Cell> cells = result2.listCells();
		
			for (Cell cell : cells) {
				System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
				System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
			}
			
		}
		
		
	}
	
	/**
	 * 创建表
	 * 
	 * @param tname
	 *            表名
	 * @param familys
	 *            列族名
	 * @throws TableExistsException 
	 */
	public static void create(String tname, String... familys) throws TableExistsException {

		if(isexist(tname)) {
			throw new TableExistsException("表已经存在");
		}
		
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tname));
		for (String family : familys) {
			desc.addFamily(new HColumnDescriptor(Bytes.toBytes(family)));
		}

		try {
			admin.createTable(desc);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void exists() throws Exception {
		
		HTable table = (HTable) con.getTable(TableName.valueOf("student"));
		
		Get get = new Get(Bytes.toBytes("row1"));
		get.addColumn(Bytes.toBytes("chengji"), Bytes.toBytes("china"));
		
		if (table.exists(get))
			System.out.println("存在");
		
	}
	
	/**
	 * 删除表中数据
	 * @param tname 表名
	 * @param rowkey 行key
	 * @param fam_qua 列族名
	 * @throws TableNotFoundException
	 */
	public static void delete(String tname, String rowkey,Map<String,String> fam_qua) throws TableNotFoundException {

		if(!isexist(tname)) {
			throw new TableNotFoundException("表不存在");
		}
		Table table = null;
		try {
			table = con.getTable(TableName.valueOf(tname));
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<Delete> deletes = new ArrayList<>();
		for (String family : fam_qua.keySet()) {
			Delete delete = new Delete(Bytes.toBytes(rowkey));
			System.out.println("family = " + family);
			System.out.println("qualifier = " + fam_qua.get(family));
			delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(fam_qua.get(family)));
			deletes.add(delete);
		}
		
		try {
			table.delete(deletes);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * 仅按照表名和行健查询信息:针对单表单行键
	 * @param tableName
	 * @param rowKey
	 * @throws IOException 
	 */
	public static List<Cell> get(String tableName,String rowKey) throws IOException{
		List<Cell> cells=null;
		TableName tn=TableName.valueOf(tableName);
		if(admin.tableExists(tn)){
			Table t=con.getTable(tn);
			Get get=new Get(Bytes.toBytes("rowKey"));//根据行键查询值
			Result result=t.get(get);
			cells=result.listCells();
		}else{
			throw new RuntimeException("您查询的表不存在");
		}
		return cells;
	}
	
	
	public static String[] getTableNameArray(){
		TableName[] tnArray = null;
		try {
			tnArray = admin.listTableNames();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String[] tables=new String[tnArray.length];
		for(int i=0;i<tnArray.length;i++){
			tables[i]=tnArray[i].getNameAsString();
		}
		return tables;
	}
	
	/**
	 * 删除指定表
	 * 
	 * @param tname
	 * @throws TableNotFoundException 
	 */
	public static void dropTable(String tname) throws TableNotFoundException {

		if(!isexist(tname)) {
			throw new TableNotFoundException("表不存在");
		}
		
		TableName t_name = TableName.valueOf(tname);
		try {
			admin.disableTable(t_name);
			admin.deleteTable(t_name);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * 判断表是否存在
	 * @param tname 表名
	 * @return 存在返回true
	 */
	private static boolean isexist(String tname) {
	
		try {
			return admin.tableExists(TableName.valueOf(tname));
		} catch (IOException e) {
			return false;
		}
		
	}
	
	/**
	 * 更新或者插入
	 * @param tname 表名
	 * @param rowkey 行key
	 * @param fam_qua_val 列族_列_值
	 * @throws TableNotFoundException
	 */
	public static void Put(String tname,String rowkey,Map<String,Map<String,String>> fam_qua_val) throws TableNotFoundException {
		
		if(!isexist(tname)) {
			throw new TableNotFoundException("表不存在");
		}
		
		TableName t_name = TableName.valueOf(tname);
		Table table = null;
		try {
			table = con.getTable(t_name);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Put put = new Put(Bytes.toBytes(rowkey));
		
		for (String family : fam_qua_val.keySet()) {
			
			for (String qualifier : fam_qua_val.get(family).keySet()) {
				
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(fam_qua_val.get(family).get(qualifier)));
				
			}
			
			
		}
		
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
