package com.zx.Trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 *
 * 此过滤器用来过滤奇偶数
 * 判断两数之和是否为偶数
 *
 */
public class CheckEventSumFilter extends BaseFilter {

	private static final long serialVersionUID = 1L;

	public boolean isKeep(TridentTuple tuple) {

		int a = tuple.getInteger(0);
		int b = tuple.getInteger(1);

		int sum = a + b;
		System.out.println("======>sum = " + sum);
		return sum % 2 == 0;
	}

}
