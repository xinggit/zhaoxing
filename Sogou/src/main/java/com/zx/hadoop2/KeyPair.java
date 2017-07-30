package com.zx.hadoop2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KeyPair implements WritableComparable<KeyPair>{

	private int frist;
	private int second;
	
	public KeyPair() {
	}
	

	public KeyPair(int frist, int second) {
		this.frist = frist;
		this.second = second;
	}

	public int getFrist() {
		return frist;
	}
	public void setFrist(int frist) {
		this.frist = frist;
	}
	public int getSecond() {
		return second;
	}
	public void setSecond(int second) {
		this.second = second;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(frist);
		out.writeInt(second);
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		frist = in.readInt();
		second = in.readInt();
	}


	@Override
	public int compareTo(KeyPair o) {
		
		int result = Integer.compare(frist, o.frist);
		
		if(result == 0) {
			return Integer.compare(second, o.second);
		}
		
		return result;
	}
	
}
