package com.zx.myhbase;

import java.util.ArrayList;
import java.util.List;

class Demo01 {

	public static void main(String[] args) {

		List<String> list = new ArrayList<>();
		list.add("a");
		list.add("b");
		list.add("c");
		list.add("d");

		for (String string : list) {
			for (String string1 : list) {

				System.out.println(string1);

			}
		}

	}

}
