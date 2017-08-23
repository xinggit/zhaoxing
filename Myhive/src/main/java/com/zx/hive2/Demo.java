package com.zx.hive2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Demo {

	public static final String url = "jdbc:mysql://192.168.112.130:3306/hive";
	public static final String name = "com.mysql.jdbc.Driver";
	public static final String user = "root";
	public static final String password = "123456";
	public static Connection conn = null;
	public static PreparedStatement pst = null;

	public static void main(String[] args) {

		try {
			Class.forName(name);// 指定连接类型
			conn = DriverManager.getConnection(url, user, password);// 获取连接
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
