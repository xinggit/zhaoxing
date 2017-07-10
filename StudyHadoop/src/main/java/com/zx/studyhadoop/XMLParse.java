package com.zx.studyhadoop;

import java.io.InputStream;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class XMLParse {
	public static void main(String[] args) throws Exception {
		InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("student.xml");
		
		SAXReader reader = new SAXReader();
		Document doc = reader.read(in);
		
		print(doc.getRootElement());
		
	}

	private static void print(Element element) {
		System.out.println(element.getName() + "--->" + element.getTextTrim());
		List<Element> list = element.elements();
		for (Element element2 : list) {
			print(element2);
		}
		
	}
}
