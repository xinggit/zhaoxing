package com.zx.myUDF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Myadd extends UDF {

	public Integer evaluate(Integer... params) {

		Integer sum = 0;
		for (Integer i : params) {
			sum += i;
		}

		return sum;
	}

}