package com.zx.hadoop4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.common.IOUtils;
import org.mortbay.log.Log;

/**
 * 使用mapfile来合并小文件
 * 
 * @author root
 *
 */
public class MapFileDemo {
	private static Configuration conf = new Configuration();

	public static void doWrite(String[] texts, String path,long index) {

		try {
			
			MapFile.Writer write = new MapFile.Writer(conf, new Path(path), MapFile.Writer.keyClass(LongWritable.class),
					MapFile.Writer.valueClass(Text.class));

			LongWritable key = new LongWritable(index);
			Text val = new Text();

			for (String string : texts) {

				val.set(string);
				write.append(key, val);
				key.set(key.get() + string.getBytes().length);

			}
			Log.info("key = " + key.get());
			IOUtils.closeStream(write);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void doReader(String path) throws IOException {

		MapFile.Reader reader = new MapFile.Reader(new Path(path), conf);
		WritableComparable<?> key = (WritableComparable<?>) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		WritableComparable<?> value = (WritableComparable<?>) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
		while(reader.next(key, value)) {
			
			Log.info("key = " + key + ", value = " + value);

		}
		
		IOUtils.closeStream(reader);
		
	}

}
