package com.zx.myhive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class HiveDemo02 {

	public static void main(String[] args) throws Exception {
		
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		
		Connection con = DriverManager.getConnection("jdbc:hive2://master:10000/stu", "hadoop", "a");
	
		String sql = "select * from emp";
		PreparedStatement pre =  con.prepareStatement(sql);
		ResultSet result = pre.executeQuery();
		
		while(result.next()) {
			System.out.println("id = " + result.getInt(1) + "name = " + result.getString(2) + "age = " + result.getInt(3));
		}
		
	}
	
}
