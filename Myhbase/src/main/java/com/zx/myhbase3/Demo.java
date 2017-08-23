package com.zx.myhbase3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Demo {

	public static final String url = "jdbc:mysql://master:3306/hive";
	public static final String name = "com.mysql.jdbc.Driver";
	public static final String user = "hive";
	public static final String password = "hive";
	public static Connection conn = null;
	public static PreparedStatement pst = null;

	public static void main(String[] args) {

		try {
			Class.forName(name);// 指定连接类型
			conn = DriverManager.getConnection(url, user, password);// 获取连接
			System.out.println("连接成功");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
