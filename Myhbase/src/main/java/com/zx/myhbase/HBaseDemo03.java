package com.zx.myhbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDemo03 {

	private static Connection con;
	private static Admin admin;
	private static Map<String, List<String>>[] family_col_val;
	static {

		Configuration conf = HBaseConfiguration.create();
		try {
			con = ConnectionFactory.createConnection(conf);
			admin = con.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {

		TableName tn = TableName.valueOf("student");
		if(admin.tableExists(tn)) {
			
			Table t = con.getTable(tn);
			Scan scan = new Scan();
			scan.setCaching(10);
			
			ValueFilter filter = new ValueFilter(CompareOp.GREATER, new RegexStringComparator("[8-9]\\d+"));
			scan.setFilter(filter);
			scan.addFamily(Bytes.toBytes("chengji"));
			ResultScanner rs = t.getScanner(scan);
			for (Result result : rs) {
				
				List<Cell> cells = result.listCells();
				
				for (Cell cell : cells) {
					String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
					String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
					String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
					String value = Bytes.toString(CellUtil.cloneValue(cell));
					System.out.println(rowkey + ", " + columnFamily + ", " + qualifier + ", " + value);
				}
				
			}
			
		}
		
	}

	public static void test01() throws TableNotFoundException {
		List<Map<String, Map<String, String>>> family_col = new ArrayList<Map<String, Map<String, String>>>();
		Map<String, Map<String, String>> e = new HashMap<String, Map<String, String>>();

		Map<String, String> map1 = new HashMap<>();
		map1.put("name", "zhaoxing");
		map1.put("qq", "1193166256");
		map1.put("age", "20");
		e.put("xinxi", map1);

		Map<String, String> map2 = new HashMap<>();

		map2.put("math", "100");
		map2.put("english", "80");
		map2.put("china", "90");
		e.put("chengji", map2);

		family_col.add(e);

		HBaseUtils.doUpdate("student", "row1", family_col);
	}

	public static void add() throws IOException {
		List<String> list = new ArrayList<>();
		list.add("xinxi");
		list.add("chengji");
		HBaseUtils.createTable("student", list);
	}

}
