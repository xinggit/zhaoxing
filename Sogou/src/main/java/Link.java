

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Link {

	public static void main(String[] args) throws Exception {
		
		Class.forName("com.mysql.jdbc.Driver");
		Connection con = DriverManager.getConnection("jdbc:mysql://192.168.112.130:3306/myhive", "root", "123456");
		
		String sql = "select * from stu";
		PreparedStatement pre = con.prepareStatement(sql);
		ResultSet result = pre.executeQuery();
		while(result.next()) {
			System.out.println("id = " + result.getInt(1) + ",name = " + result.getString(2));
		}
	}
	
}
