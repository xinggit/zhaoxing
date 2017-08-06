package com.zx.serde;

public class Demo {

	public static void main(String[] args) {
		
		String a = "<DOC>id=4name=dfwete5y</DOC>";
		
		String b = a.substring(5, a.lastIndexOf("/") - 1);
		System.out.println(b);
	}
	
}
