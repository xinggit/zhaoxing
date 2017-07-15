package com.zx.studyhadoop2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map02 extends Mapper<LongWritable, Text, Text, Text> {

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		
		String[] arg = line.split("\t");
		
		context.write(new Text(arg[0]), new Text(arg[1]));
		context.write(new Text(arg[1]), new Text(arg[0]));
	}

}
