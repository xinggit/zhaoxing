package com.zx.hadoop2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Mytext implements WritableComparable<Mytext>{

	private String content;
	
	public Mytext() {
	}
	
	public Mytext(String content) {
		this.content = content;
	}
	
	@Override  //序列化过程
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(content);
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		content = in.readUTF();
	}

	@Override
	public int compareTo(Mytext o) {
		System.out.println("this.content = " + content + ",content = " + o.content );
//		return this.compareTo(o);
		return content.compareTo(o.content);//此处为何能调用到私有属性?
	}
	
	@Override
	public String toString() {
		return content;
	}

	
}
