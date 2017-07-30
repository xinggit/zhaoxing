package com.zx.hadoop3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Mysort2 extends WritableComparator{

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		return super.compare(a, b);
	}

	
	
}
