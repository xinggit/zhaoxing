package com.zx.CFJ;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class Demo01 {
	public static void main(String[] args) {
		URL url = null;
		
		try {
			url = new URL("http://www.meweidea.com/");
			Document doc = Jsoup.parse(url,3000);
//			System.out.println(doc.head());
//			System.out.println(doc.data());
			System.out.println(doc.body());
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
