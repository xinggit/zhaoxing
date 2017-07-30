package com.zx.hadoop3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class MySort implements RawComparator<IntWritable>{

	@Override
	public int compare(IntWritable o1, IntWritable o2) {
		
		return Integer.compare(o1.get(), o2.get());
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

		return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
	}

}
