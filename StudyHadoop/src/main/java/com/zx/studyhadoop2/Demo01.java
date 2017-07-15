package com.zx.studyhadoop2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.io.Text;

public class Demo01 {
	public static void main(String[] args) {
		InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("Friend.txt");
		
		ByteArrayOutputStream ny = new ByteArrayOutputStream();
		
		byte[] by = new byte[1024];
		int len = -1;
		try {
			while((len = in.read(by)) != -1) {
				ny.write(by, 0, len);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String line = new String(ny.toByteArray());
		
		String[] arg = line.split("\t");
	
		for (String string : arg) {
			System.out.println(string);
		}
	
	}
}
