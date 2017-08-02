package com.zx.myUDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

public class To_date extends UDF {

	public Date evaluate(String date) {

		SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

		try {
			return sim.parse(date);
		} catch (ParseException e) {
			return null;
		}

	}

}
