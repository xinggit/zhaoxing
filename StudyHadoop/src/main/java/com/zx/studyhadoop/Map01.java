package com.zx.studyhadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *  此程序对应的列子为wordcount
 *  对应关系
 *	LongWritable   ---->    long
 *  Text           ---->    String
 *  IntWritable    ---->    int
 */

public class Map01 extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	//每次调用map方法,会传入split中的一行
	//key:该行所在文件中的位置下标
	//value:该行数据
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		super.map(key, value, context);
		
		String line = value.toString();
		
		StringTokenizer st = new StringTokenizer(line, "\t\n\r\f,");
		
		while(st.hasMoreTokens()) {
			context.write(new Text(st.nextToken()), new IntWritable(1));
		}
		
	}
}
