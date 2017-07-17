package com.zx.hadoop6;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.log.Log;

public class SequenceFileDemo {

	private static Configuration conf = new Configuration();

	public static void doWrite(String path, String[] texts) {

		try {
			
			SequenceFile.Writer write = SequenceFile.createWriter(conf,SequenceFile.Writer.file(new Path(path)),
					SequenceFile.Writer.keyClass(LongWritable.class), SequenceFile.Writer.valueClass(Text.class));
		
			
			Text val = new Text();
			LongWritable key = new LongWritable();
			
			for (String string : texts) {
				
				val.set(string);
				write.append(key, val);
				key.set(string.getBytes().length + key.get());
				
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void doRead(String path) {
		
		try {
			
			SequenceFile.Reader read = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(path)));
			WritableComparable<?> key = (WritableComparable<?>) ReflectionUtils.newInstance(read.getKeyClass(), conf);
			WritableComparable<?> value = (WritableComparable<?>) ReflectionUtils.newInstance(read.getValueClass(), conf);
			
			while(read.next(key, value)) {
				
				Log.info("key = " + key + ",value = " + value);
				
			}
			
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
		
	}

}
