package com.zx.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DataType implements WritableComparable<DataType> {

	private String date;
	private String num;

	public DataType() {
	}

	public DataType(String date, String data) {
		this.date = date;
		this.num = data;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getData() {
		return num;
	}

	public void setData(String data) {
		this.num = data;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(date);
		out.writeUTF(num);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.date = in.readUTF();
		this.num = in.readUTF();
	}

	@Override
	public int compareTo(DataType o) {

		if (date.compareTo(o.date) == 0 && num.compareTo(o.num) == 0) {
			return 0;
		}

		return -1;
	}

}
