package com.zx.myhbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class MyMapper extends TableMapper<Text, Text> {

	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		List<Cell> cells = value.listCells();

		String k = null;
		String v = null;
		
		for (Cell cell : cells) {
			
			if (CellUtil.matchingQualifier(cell, Bytes.toBytes("tags"))) {
				k = Bytes.toString(CellUtil.cloneValue(cell));
			}
			
			if (CellUtil.matchingQualifier(cell, Bytes.toBytes("nickname"))) {
				v = Bytes.toString(CellUtil.cloneValue(cell));
			}
			
		}

		String[] ks = k.split(",");
		
		for (String string : ks) {
			context.write(new Text(string), new Text(v));
		}
		
		
	}

}
