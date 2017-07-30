package com.zx.example8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DataHot implements WritableComparable<DataHot> {

	private int data;
	private int hot;
	private int year;
	public DataHot() {
	}

	public DataHot(int data, int hot) {
		this.data = data;
		this.hot = hot;
		year = Integer.parseInt((this.hot + "").substring(0, 4));
	}

	public int getData() {
		return data;
	}

	public void setData(int data) {
		this.data = data;
	}

	public int getHot() {
		return hot;
	}

	public void setHot(int hot) {
		this.hot = hot;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(data);
		out.writeInt(hot);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.hot = in.readInt();
		this.data = in.readInt();
	}

	@Override
	public int compareTo(DataHot o) {

		int result = Integer.compare(this.year, o.year);

		if (result == 0) {
			return -Integer.compare(this.hot, o.hot);
		}

		return result;
	}

	@Override
	public int hashCode() {
		return year % 8;
	}

}
