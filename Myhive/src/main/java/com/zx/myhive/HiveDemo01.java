package com.zx.myhive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveDemo01 {
	private static Connection con;
	static {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			con = DriverManager.getConnection("jdbc:hive2://master:10000/stu", "hadoop", "a");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}  

	public static void main(String[] args) throws Exception {
//		insert();
		select();
	}

	public static void insert() throws Exception {
		
		String sql = "insert into stu values(?,?)";
		PreparedStatement pre = con.prepareStatement(sql);
		pre.setInt(1, 100002);
		pre.setString(2,"赵星");
		pre.execute();
		pre.close();
		
	}
	
	public static void select() throws SQLException {
		String sql = "select * from stu";
		PreparedStatement pre = con.prepareStatement(sql);
		ResultSet result = pre.executeQuery();
		while (result.next()) {
			int id = result.getInt(1);
			String name = result.getString(2);
			System.out.println("id = " + id + ",name = " + name);
		}

		result.close();
		pre.close();
		con.close();
	}

}
