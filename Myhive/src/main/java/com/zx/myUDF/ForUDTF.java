package com.zx.myUDF;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

/**
 * 简单的for循环
 *
 */
public class ForUDTF extends GenericUDTF{

	@Override
	public void process(Object[] args) throws HiveException {
		
	}

	@Override
	public void close() throws HiveException {
		
	}

}
