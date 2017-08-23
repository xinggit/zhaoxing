package com.zx.myUDF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class map_key extends UDF {

	public String evaluate(String date) {

		String val = date.substring(1, date.length()-1);
		String[] kv = val.split(",");
		for (String key : kv) {
			String[] k = key.split(":");
			if(k.length == 2){
				return k[0].substring(1, k[0].length()-1);
			}
		}
		return null;
		
	}

}
