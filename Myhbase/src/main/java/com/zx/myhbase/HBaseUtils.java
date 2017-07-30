package com.zx.myhbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtils {

	private static Connection con;
	private static Admin admin;
	static {

		Configuration conf = HBaseConfiguration.create();
		try {
			con = ConnectionFactory.createConnection(conf);
			admin = con.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param table_name
	 *            表名
	 * @param family_column
	 *            列族
	 * @throws IOException
	 */
	public static void createTable(String table_name, List<String> family_column) throws IOException {

		TableName name = TableName.valueOf(table_name);
		if (!admin.tableExists(name)) {

			HTableDescriptor desc = new HTableDescriptor(name);

			// 将多个列族加入其中
			for (String familyName : family_column) {

				HColumnDescriptor family = new HColumnDescriptor(familyName);
				desc.addFamily(family);

			}

			admin.createTable(desc);
			admin.close();
			con.close();
		} else {
			throw new TableExistsException();
		}
	}

	public static String get(String tableName, String rowKey, String columnFamily, String qualifier) {

		try {
			Table t = con.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));

			get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
			Result r = t.get(get);
			List<Cell> clees = r.listCells();
			return Bytes.toString(CellUtil.cloneValue(clees.get(0)));

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}

	public static Map<String, String> get(String tableName, String rowKey, String columnFamily, String... qualifiers) {
		try {
			Table t = con.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			if (qualifiers != null && qualifiers.length != 0) {
				for (String qualifier : qualifiers) {
					get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
				}
			} else if (columnFamily != null) {
				get.addFamily(Bytes.toBytes(columnFamily));
			}
			Result r = t.get(get);
			List<Cell> cells = r.listCells();
			Map<String, String> results = null;
			if (cells != null && cells.size() != 0) {
				results = new HashMap<String, String>();
				for (Cell cell : cells) {
					results.put(Bytes.toString(CellUtil.cloneQualifier(cell)),
							Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}
			return results;
		} catch (IOException e) {
			throw new RuntimeException("获取表对象失败!!");
		}
	}

	public static void doUpdate(String tableName, MutationType mt, Map<String, List<String[]>> params)
			throws IOException {

		TableName tn = TableName.valueOf(tableName);

		if (admin.tableExists(tn)) {

			Table t = con.getTable(tn);
			switch (mt) {
			case PUT:
				List<Put> puts = new ArrayList<Put>();
				for (Entry<String, List<String[]>> entry : params.entrySet()) {

					Put put = new Put(Bytes.toBytes(entry.getKey()));
					for (String[] ps : entry.getValue()) {
						if (ps.length == 3) {
							put.addColumn(Bytes.toBytes(ps[0]), Bytes.toBytes(ps[1]), Bytes.toBytes(ps[2]));
						} else {
							throw new RuntimeException("参数个数为三个");
						}
					}
					puts.add(put);
				}
			case DELETE:
				List<Delete> dels = new ArrayList<Delete>();
				for (Entry<String, List<String[]>> entry : params.entrySet()) {
					Delete del = new Delete(Bytes.toBytes(entry.getKey()));
					if (entry.getValue() != null) {

						for (String[] ps : entry.getValue()) {
							if (params != null && ps.length != 0) {
								switch (ps.length) {

								case 1:
									del.addFamily(Bytes.toBytes(ps[0]));
									break;
								case 2:
									del.addColumn(Bytes.toBytes(ps[0]), Bytes.toBytes(ps[1]));
									break;
								default:
									throw new RuntimeException("最多两个参数");
								}

							}
						}

					}
					dels.add(del);
				}
				t.delete(dels);
				break;
			default:
				break;
			}
		}

	}

	/**
	 * 增加或者更新
	 * 
	 * @param table_name
	 *            表名
	 * @param row
	 *            行key
	 * @param family_col
	 *            列族 + 列 + 值
	 * @throws TableNotFoundException
	 */
	public static void doUpdate(String table_name, String row, List<Map<String, Map<String, String>>> family_col)
			throws TableNotFoundException {

		TableName tableName = TableName.valueOf(table_name);
		Table table;
		try {
			table = con.getTable(tableName);
		} catch (IOException e) {
			throw new TableNotFoundException();
		}
		Put put = new Put(row.getBytes());

		// 得到列族名+列名+值
		for (Map<String, Map<String, String>> map : family_col) {

			// key = 列祖,value = 列名+值
			for (String family : map.keySet()) {

				// key = 列名,value = 值
				for (String col_name : map.get(family).keySet()) {

					put.addColumn(Bytes.toBytes(family), Bytes.toBytes(col_name),
							Bytes.toBytes(map.get(family).get(col_name)));

				}

			}

		}

		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
