package com.zx.sqoop;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.validation.Status;

public class SqoopDemo {

	public static void main(String[] args) {

		SqoopClient client = new SqoopClient("http://master:12000/sqoop/");

		// create a placeholder for link
		MLink link = client.createLink("connectorName");
		link.setName("api_jdbc");
		MLinkConfig linkConfig = link.getConnectorLinkConfig();
		
		
		// fill in the link config values
		linkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:mysql://master/sqoop");
		linkConfig.getStringInput("linkConfig.jdbcDriver").setValue("com.mysql.jdbc.Driver");
		linkConfig.getStringInput("linkConfig.username").setValue("root");
		linkConfig.getStringInput("linkConfig.password").setValue("123456");
		// save the link object that was filled
		Status status = client.saveLink(link);
		if (status.canProceed()) {
			System.out.println("Created Link with Link Name : " + link.getName());
		} else {
			System.out.println("Something went wrong creating the link");
		}

	}

}
