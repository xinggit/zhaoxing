package com.zx.hadoop3;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.log.Log;

public class SequenceFileDemo {

	private static Configuration conf = new Configuration();

	// 使用SequenceFileDemo合并小文件
	public static void doWrite(String[] texts, String strpath) {

		try {
			SequenceFile.Writer write = SequenceFile.createWriter(conf, SequenceFile.Writer.file(new Path(strpath)),
					SequenceFile.Writer.keyClass(LongWritable.class), SequenceFile.Writer.valueClass(Text.class));

			LongWritable key = new LongWritable();
			Text value = new Text();

			StringBuilder str = new StringBuilder();

			for (String text : texts) {

				key.set(str.length());
				value.set(text);

				Log.info("key = " + key + ",value = " + value);

				write.append(key, value);
				
				str.append(text);

			}

			IOUtils.closeQuietly(write);

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// 使用SequenceFiley读取小文件
	public static void doRead(String srcpath) {

		SequenceFile.Reader reader;
		try {

			reader = new SequenceFile.Reader(conf, Reader.file(new Path(srcpath)));
			
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

//			reader.getPosition();
			while (reader.next(key, value)) {
				System.out.println("key = " + key + ", value = " + value);
//				reader.getPosition();
			}

			IOUtils.closeQuietly(reader);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}