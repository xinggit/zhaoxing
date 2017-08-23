package com.zx.hive2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


public class HiveDemo01 {

	private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
	private static final String URL = "jdbc:hive2://master:10000/stu";
	private static final String USER = "hive";
	private static final String PASSWD = "hive";
	
	
	public static void main(String[] args) throws Exception {
		
		Class.forName(DRIVER);
		
		Connection con = DriverManager.getConnection(URL, USER, PASSWD);
		
		String sql = "select * from cus";
		PreparedStatement pre = con.prepareStatement(sql);
		
		ResultSet res = pre.executeQuery();
		
		while(res.next()) {
			
			System.out.println(res.getObject(1));
			
		}
	
		res.close();
		pre.close();
		con.close();
		
		
	}
	
}
