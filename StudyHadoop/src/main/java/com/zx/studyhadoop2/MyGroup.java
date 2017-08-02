package com.zx.studyhadoop2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator {
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
	
		return 0;
		
	}
	
}
