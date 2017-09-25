package com.zx.Storm_kafka;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class StreamUtil {

	public static void Write(String fileName,String line) {
		
		File file = new File(fileName);
		
		if(!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		try {
			FileWriter out = new FileWriter(file);
			
			out.write(line + "\n");
			out.flush();
			out.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
