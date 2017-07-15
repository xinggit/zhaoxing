package com.zx.hadoop2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class HadoopTypeDemo {

	public static void main(String[] args) throws Exception {
		
		//测试序列化数据
		//测试第一种情况:测试字符串string
		//测试第二章情况:测试整形数据int
		
		
//		Text text = new Text("你好");
//		byte[] bs = serializa(text);
		
		IntWritable in = new IntWritable(123);
		byte[] bs = serializa(in);
		
		System.out.println(bs.length + "=" + new String(bs));
		
//		Text text1 = new Text();
//		System.out.println(deserialize(text1, bs));
		
		IntWritable in1 = new IntWritable();
		
		System.out.println(deserialize(in1, bs));
	}

	// 序列化操作: 结构化数据转化成字节数据流
	public static byte[] serializa(Writable w) throws IOException {

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream data = new DataOutputStream(out);

		w.write(data);

		data.flush();
		data.close();

		return out.toByteArray();

	}

	// 反序列化: 字节数据流转化成结构化数据
	public static String deserialize(Writable w, byte[] bs) throws IOException {

		ByteArrayInputStream in = new ByteArrayInputStream(bs);
		DataInputStream data = new DataInputStream(in);

		w.readFields(data);

		data.close();

		return w.toString();

	}

}
