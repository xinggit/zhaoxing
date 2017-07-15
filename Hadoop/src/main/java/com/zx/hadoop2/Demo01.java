package com.zx.hadoop2;

public class Demo01 {
	public static void main(String[] args) {
		Mytext text = new Mytext("张三");
		
		Mytext text2 = new Mytext("李四");
		
		System.out.println(text.compareTo(text2));
		
	}
	
	
}
