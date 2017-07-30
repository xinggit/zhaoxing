package com.zx.myhbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.log.Log;

public class MyMapper extends TableMapper<ImmutableBytesWritable, Result> {

	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		super.map(key, value, context);
		
		Log.info("================key = " + Bytes.toString(key.get()));
		List<Cell> cell = value.listCells();
			
		for (Cell cell2 : cell) {
			Log.info("value" + CellUtil.cloneValue(cell2));
		}
		
	}

}
