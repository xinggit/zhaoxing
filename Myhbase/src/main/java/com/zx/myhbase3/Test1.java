package com.zx.myhbase3;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;

public class Test1 {

	public static void main(String[] args) {
		test1();
	}

	public static void test1() {
		try {
			HbaseUtil.create("zx:t1", "info","course");
		} catch (Exception e) {
			System.out.println("表已存在");
		}
		
	}

	public static void test2() {
		Map<String,Map<String,String>> map = new HashMap<>();
		Map<String,String> map2 = new HashMap<>();
		map.put("xinxi", map2);
		
		for (int i = 0; i < 10; i++) {
			map2.put("age" + i, i*10 + "");
		}
		try {
			HbaseUtil.Put("zhaoxing", "row1",map );
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
	}
	
}
