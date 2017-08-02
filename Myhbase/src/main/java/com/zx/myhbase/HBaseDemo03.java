package com.zx.myhbase;

import java.io

		.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

/*	
 * 
 * 
 * // 分页操作
 *	int pageSize = 3; // 每页的数据条数
 *	int pagNum = 2; // 第几页
 * 
 * 
 * 
 * */
public class HBaseDemo03 {
	public static void main(String[] args) throws IOException {

		pagenation(7, 2);
	}

	public static void pagenation(int pageSize, int pagNum) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "slave01,slave02,slave03"); // 设置hbase由那个zookeeper集群协调管理

		ExecutorService pool = Executors.newCachedThreadPool();
		Connection con = ConnectionFactory.createConnection(conf, pool);
		Table t = con.getTable(TableName.valueOf("student"), pool);

		// 查询数据 ----扫描器scan
		Scan scan = new Scan(); // 创建表的扫描器对象
		PageFilter pageFilter = new PageFilter(pageSize); // 创建页面过滤器，设置过滤页面的条数
		// pageFilter.
		// 怎样取到每一页的第一条数据
		ResultScanner rs = t.getScanner(scan);
		
		
		Result res = null;
		Iterator<Result> iterator = rs.iterator();
		for (int i = 0; i < pagNum - 1; i++) {
			for (int j = 0; j < pageSize + 1 ; j++) {
				if(iterator.hasNext())
					res = iterator.next();
				else
					break;
			}

		}
		scan.setStartRow(res.getRow());
		scan.setFilter(pageFilter);
		rs = t.getScanner(scan);
		// 取到指定页面的内容
		for (Result result : rs) { // 有多少个行键就循环多少次
			List<Cell> cells = result.listCells(); // 每一个行键只有一个List<Cell>
			if (cells != null && cells.size() != 0) {
				for (Cell cell : cells) {
					String rowkey = Bytes.toString(CellUtil.cloneRow(cell)); // CellUtil对单元格操作的工具
					String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
					String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
					String value = Bytes.toString(CellUtil.cloneValue(cell));
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss E");
					String timeStr = sdf.format(new Date(cell.getTimestamp()));
					String str = String.format("行键值:%s,列族名：%s,单元格修饰名:%s,单元格的值:%s,时间戳:%s", rowkey, columnFamily,
							qualifier, value, timeStr);
					System.out.println(str);
				}
				System.out.println("----------------------------------------------------");
			}
		}
		
		
		rs.close();
		con.close();
		pool.shutdown();
	}
}