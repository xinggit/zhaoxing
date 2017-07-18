package com.zx.hadoop;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.zx.bean.Place;
import com.zx.bean.Univercity;

public class Reduce extends Reducer<Text, Text, Text, IntWritable> {
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws java.io.IOException, InterruptedException {
		
		List<Univercity> uns = new ArrayList<Univercity>();
		Place place = new Place(key.toString(), uns);
		
		for (Text text : value) {
			String[] v = text.toString().split(",");
			uns.add(new Univercity(v[0], v[1]));
		}
		
		context.write(key, new IntWritable(uns.size()));
		
	};
}
