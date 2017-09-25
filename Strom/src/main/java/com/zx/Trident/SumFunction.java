package com.zx.Trident;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * 此函数的作用
 *
 *	old tuple {1,2,3,4} ---> function ---> new tuple{1,2,3,4,3}
 *	old tuple {5,6,7,8} ---> function ---> new tuple{5,6,7,8,11}
 *
 *	即将前两个数字相加合并成一个新的数据，然后将其变为一个字段，生成新的tuple
 *
 */
public class SumFunction extends BaseFunction{

	private static final long serialVersionUID = 2918710401493565690L;

	public void execute(TridentTuple tuple, TridentCollector collector) {
		
		int a = tuple.getInteger(0);
		int b = tuple.getInteger(1);
		
		int sum = a + b;
		collector.emit(new Values(sum));
		
		
	}

}
